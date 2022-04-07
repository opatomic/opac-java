/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.opatomic;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

final class Request {
	public final CharSequence command;
	public final Iterator<?> args;
	public final Object asyncId;
	public final CallbackSF<Object,OpaRpcError> cb;

	Request(CharSequence command, Iterator<?> args, Object asyncId, CallbackSF<Object,OpaRpcError> cb) {
		if (command == null) {
			throw new IllegalArgumentException();
		}
		this.command = command;
		this.args = args;
		this.asyncId = asyncId;
		this.cb = cb;
	}

	// TODO: implement toString()?

	// TODO: return an instance of this object from call() functions with a cancel() function: if request hasn't
	//  been sent then set flag and when it is time to serialize, call failure callback? see java.util.concurrent.Future
}

/**
 * Opatomic client that uses 2 threads: 1 for parser and 1 for serializer. Methods do not block.
 * Cannot modify args until callback is invoked (because requests are serialized in separate thread).
 */
public class OpaStreamClient implements OpaClient {

	private static final Request LASTREQUEST = new Request("", null, null, null);
	private static final OpaRpcError CLOSED_ERROR = new OpaRpcError(OpaDef.ERR_CLOSED);

	private final AtomicLong mCurrId = new AtomicLong();

	private final OpaSerializer mSerializer;
	private final BlockingQueue<Request> mSerializeQueue = new LinkedBlockingQueue<Request>();

	private final Queue<CallbackSF<Object,OpaRpcError>> mMainCallbacks = new ConcurrentLinkedQueue<CallbackSF<Object,OpaRpcError>>();
	private final Map<Object,CallbackSF<Object,OpaRpcError>> mAsyncCallbacks = new ConcurrentHashMap<Object,CallbackSF<Object,OpaRpcError>>();

	private boolean mQuit = false;
	private boolean mQuitting = false;
	private boolean mClosed = false;

	/**
	 * Create a new client that will serialize requests to an OutputStream and parse responses from an InputStream.
	 * @param in  Stream to parse responses
	 * @param out Stream to serialize requests
	 */
	public OpaStreamClient(final InputStream in, final OutputStream out) {
		mSerializer = new OpaSerializer(out, 1024 * 8);

		// TODO: consider using java.util.concurrent.Executor for send? (recv will always be blocking or doing work)

		OpaUtils.startDaemonThread(new Runnable() {
			public void run() {
				try {
					serializeRequests(mSerializeQueue);

					// the only way for serializeRequests() to return is when parser is done, has queued LASTREQUEST
					// and the serializer has received LASTREQUEST. therefore, queue another LASTREQUEST for
					// the upcoming call to cleanupDeadRequests()
					// mClosed should have been set by recv thread
					mSerializeQueue.add(LASTREQUEST);
				} catch (Exception e) {
					e.printStackTrace();

					// at this point, the recv thread may be running or may be closed.
					// close InputStream to indicate that serializer is done and recv thread must stop
					// (if it hasn't stopped already). when recv thread stops, it queues LASTREQUEST
					// which eventually informs cleanupDeadRequests() that it can stop running.
					// note: this will cause any incoming responses to not be parsed and onFailure() to be
					// invoked for each waiting request's callback.
					// TODO: if recv thread is still running and there's pending callbacks in
					//  mMainCallbacks or mAsyncCallbacks, add a timeout to allow more responses
					//  to be parsed and handled?
					// TODO: test this
					mClosed = true;
					try {
						in.close();
					} catch (Exception e2) {}
				}

				cleanupDeadRequests(mSerializeQueue);
				respondWithClosedErr(mMainCallbacks, mAsyncCallbacks);
				//OpaDef.log("closing send thread");
			}
		}, "OpaStreamClient-send");

		OpaUtils.startDaemonThread(new Runnable() {
			public void run() {
				try {
					parseResponses(in, 1024 * 8);
				} catch (Exception e) {
					e.printStackTrace();
				}
				mClosed = true;
				// signal send thread that recv thread is done
				mSerializeQueue.add(LASTREQUEST);
				//OpaDef.log("closing recv thread");
			}
		}, "OpaStreamClient-recv");
	}

	private static void cleanupDeadRequests(BlockingQueue<Request> q) {
		while (true) {
			try {
				Request r = q.take();
				if (r == LASTREQUEST) {
					break;
				}
				if (r != null && r.cb != null) {
					r.cb.onFailure(CLOSED_ERROR);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void respondWithClosedErr(Queue<CallbackSF<Object,OpaRpcError>> mainCBs, Map<Object,CallbackSF<Object,OpaRpcError>> asyncCBs) {
		// notify callbacks that conn is closed
		while (true) {
			CallbackSF<Object,OpaRpcError> cb = mainCBs.poll();
			if (cb == null) {
				break;
			}
			cb.onFailure(CLOSED_ERROR);
		}

		if (asyncCBs.size() > 0) {
			Iterator<Map.Entry<Object,CallbackSF<Object,OpaRpcError>>> it = asyncCBs.entrySet().iterator();
			while (it.hasNext()) {
				CallbackSF<Object,OpaRpcError> cb = it.next().getValue();
				if (cb != null) {
					cb.onFailure(CLOSED_ERROR);
				}
			}
			asyncCBs.clear();
		}
	}

	static void writeRequest(OpaSerializer s, CharSequence cmd, Iterator<?> args, Object id) throws IOException {
		s.write(OpaDef.C_ARRAYSTART);
		s.writeObject(id);
		s.writeString(cmd);
		if (args != null) {
			while (args.hasNext()) {
				s.writeObject(args.next());
			}
		}
		s.write(OpaDef.C_ARRAYEND);
	}

	private void sendRequest(Request r) throws IOException {
		if (r.asyncId == null) {
			mMainCallbacks.add(r.cb);
		}

		writeRequest(mSerializer, r.command, r.args, r.asyncId);
	}

	private void serializeRequests(BlockingQueue<Request> q) throws IOException, InterruptedException {
		Request r;
		while (true) {
			if ((r = q.poll()) == null) {
				// yield and try again to prevent unnecessary flush
				Thread.yield();
				if ((r = q.poll()) == null) {
					// make sure to flush before waiting for the next response to send
					mSerializer.flush();
					r = q.take();
				}
			}
			if (r == LASTREQUEST) {
				// this is a message from the recv thread that it is done parsing, send thread must stop too
				break;
			}
			sendRequest(r);
		}
		mSerializer.flush();
	}

	private void parseResponses(InputStream in, int buffLen) throws IOException {
		OpaClientRecvState s = new OpaClientRecvState(mMainCallbacks, mAsyncCallbacks);
		byte[] buff = new byte[buffLen];
		while (!mQuit) {
			int numRead = in.read(buff);
			if (numRead == -1) {
				break;
			}
			s.onRecv(buff, 0, numRead);
		}
	}

	private void addRequest(CharSequence command, Iterator<?> args, Object id, CallbackSF<Object,OpaRpcError> cb) {
		mSerializeQueue.add(new Request(command, args, id, cb));
	}

	private void checkState() {
		if (mClosed) {
			throw new IllegalStateException("closed");
		}
		if (mQuitting) {
			throw new IllegalStateException("quitting");
		}
	}

	@Override
	public void call(CharSequence cmd, Iterator<?> args, CallbackSF<Object,OpaRpcError> cb) {
		checkState();
		addRequest(cmd, args, cb == null ? Boolean.FALSE : null, cb);
	}

	@Override
	public void callA(CharSequence cmd, Iterator<?> args, CallbackSF<Object,OpaRpcError> cb) {
		if (cb == null) {
			throw new IllegalArgumentException("callback cannot be null");
		}
		checkState();
		Long id = mCurrId.incrementAndGet();
		mAsyncCallbacks.put(id, cb);
		addRequest(cmd, args, id, null);
	}

	@Override
	public CallbackSF<Object,OpaRpcError> registerCB(Object id, CallbackSF<Object, OpaRpcError> cb) {
		if (cb == null) {
			return mAsyncCallbacks.remove(id);
		} else {
			return mAsyncCallbacks.put(id, cb);
		}
	}

	@Override
	public void callID(Object id, CharSequence cmd, Iterator<?> args) {
		if (id == null) {
			throw new IllegalArgumentException("id cannot be null");
		}
		checkState();
		addRequest(cmd, args, id, null);
	}

	/**
	 * Queue a command that will:
	 *   1) close the send thread after the command has been written; no more commands will be sent
	 *   2) close the recv thread after the command's response has been parsed and the callback has been invoked
	 * @param cmd  Command to run (ie, QUIT)
	 * @param args Command's parameters. Do not modify
	 * @param cb   Callback to invoke when response is received
	 */
	public void quit(CharSequence cmd, Iterator<?> args, final CallbackSF<Object,OpaRpcError> cb) {
		checkState();
		mQuitting = true;
		addRequest(cmd, args, null, new CallbackSF<Object,OpaRpcError>() {
			@Override
			public void onSuccess(Object result) {
				mQuit = true;
				if (cb != null) {
					cb.onSuccess(result);
				}
			}
			@Override
			public void onFailure(OpaRpcError error) {
				if (cb != null) {
					cb.onFailure(error);
				}
			}
		});
	}
}
