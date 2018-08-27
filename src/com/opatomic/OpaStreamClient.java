package com.opatomic;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

final class Request {
	public final String command;
	public final Iterator<Object> args;
	public final long id;
	public final CallbackSF<Object,Object> cb;

	Request(String command, Iterator<Object> args, long id, CallbackSF<Object,Object> cb) {
		if (command == null) {
			throw new IllegalArgumentException();
		}
		this.command = command;
		this.args = args;
		this.id = id;
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
public class OpaStreamClient implements OpaClient<Object,Object> {

	private static final Request CLOSESENDTHREAD = new Request("", null, 0, null);


	private final AtomicLong mCurrId = new AtomicLong();

	private final OpaSerializer mSerializer;
	private final BlockingQueue<Request> mSerializeQueue = new LinkedBlockingQueue<Request>();

	private final Queue<CallbackSF<Object,Object>> mMainCallbacks = new LinkedBlockingQueue<CallbackSF<Object,Object>>();
	private final Map<Long,CallbackSF<Object,Object>> mAsyncCallbacks = new ConcurrentHashMap<Long,CallbackSF<Object,Object>>();

	private boolean mQuit = false;
	private CallbackSF<Object,Object> mQuitCB1;

	private final CallbackSF<Object,Object> mQuitCB2 = new CallbackSF<Object,Object>() {
		@Override
		public void onSuccess(Object result) {
			mQuit = true;
			if (mQuitCB1 != null) {
				mQuitCB1.onSuccess(result);
			}
		}
		@Override
		public void onFailure(Object error) {
			if (mQuitCB1 != null) {
				mQuitCB1.onFailure(error);
			}
		}
	};
	

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
				} catch (Exception e) {
					e.printStackTrace();
				}
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
				//OpaDef.log("closing recv thread");
			}
		}, "OpaStreamClient-recv");
	}

	static void writeRequest(OpaSerializer s, String cmd, Iterator<Object> args, long id, boolean noResponse) throws IOException {
		s.write(OpaDef.C_ARRAYSTART);
		s.writeString(cmd);
		if (args != null) {
			s.write(OpaDef.C_ARRAYSTART);
			while (args.hasNext()) {
				s.writeObject(args.next());
			}
			s.write(OpaDef.C_ARRAYEND);
		}
		if (id != 0) {
			if (args == null) {
				s.write(OpaDef.C_NULL);
			}
			s.writeLong(id);
		} else if (noResponse) {
			if (args == null) {
				s.write(OpaDef.C_NULL);
			}
			s.write(OpaDef.C_NULL);
		}
		s.write(OpaDef.C_ARRAYEND);
	}
	
	private void sendRequest(Request r) throws IOException {
		if (r.cb != null) {
			if (r.id != 0) {
				mAsyncCallbacks.put(r.id, r.cb);
			} else {
				mMainCallbacks.add(r.cb);
			}
		}
		
		writeRequest(mSerializer, r.command, r.args, r.id, r.cb == null);
	}

	private void serializeRequests(BlockingQueue<Request> q) throws IOException, InterruptedException {
		Request r;
		//long numSent = 0;
		do {
			if ((r = q.poll()) == null) {
				// yield and try again to prevent unnecessary flush
				Thread.yield();
				if ((r = q.poll()) == null) {
					// make sure to flush before waiting for the next response to send
					mSerializer.flush();
					r = q.take();
				}
			}
			if (r == CLOSESENDTHREAD) {
				break;
			}
			sendRequest(r);
			//++numSent;
		} while (r.cb != mQuitCB2);

		mSerializer.flush();
		//OpaDef.log("stopped sending and flushed");
	}

	private void parseResponses(InputStream in, int buffLen) throws IOException {
		
		OpaClientRecvState s = new OpaClientRecvState(mMainCallbacks, mAsyncCallbacks);
		byte[] buff = new byte[buffLen];
		while (!mQuit) {
			int numRead;
			try {
				numRead = in.read(buff);
			} catch (IOException e) {
				//if (!(e instanceof SocketException && e.getMessage().equals("Connection reset"))) {
					e.printStackTrace();
				//}
				break;
			}
			if (numRead == -1) {
				break;
			}
			s.onRecv(buff, 0, numRead);
		}

		// signal send thread to close
		mSerializeQueue.add(CLOSESENDTHREAD);

		// notify callbacks that conn is closed
		while (true) {
			CallbackSF<Object,Object> cb = mMainCallbacks.poll();
			if (cb == null) {
				break;
			}
			cb.onFailure("closed");
		}

		if (mAsyncCallbacks.size() > 0) {
			//OpaDef.log("Callback exists after done parsing");
			Iterator<Map.Entry<Long,CallbackSF<Object,Object>>> it = mAsyncCallbacks.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Long,CallbackSF<Object,Object>> e = it.next();
				if (e.getKey().longValue() > 0) {
					CallbackSF<Object,Object> cb = e.getValue();
					if (cb != null) {
						cb.onFailure("closed");
					}
				}
			}
		}

		/*
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		*/
	}

	private void addRequest(String command, Iterator<Object> args, long id, CallbackSF<Object,Object> cb) {
		mSerializeQueue.add(new Request(command, args, id, cb));
	}

	// TODO: call methods should check whether client is closed (before adding to serialize queue); if so then throw exception!

	@Override
	public void call(String cmd, Iterator<Object> args, CallbackSF<Object,Object> cb) {
		if (mQuitCB1 != null) {
			throw new IllegalStateException();
		}
		addRequest(cmd, args, 0, cb);
	}

	@Override
	public void callA(String cmd, Iterator<Object> args, CallbackSF<Object,Object> cb) {
		if (mQuitCB1 != null) {
			throw new IllegalStateException();
		}
		if (cb == null) {
			throw new IllegalArgumentException("callback cannot be null");
		}
		addRequest(cmd, args, mCurrId.incrementAndGet(), cb);
	}

	@Override
	public Object callAP(String cmd, Iterator<Object> args, CallbackSF<Object,Object> cb) {
		if (mQuitCB1 != null) {
			throw new IllegalStateException();
		}
		if (cb == null) {
			throw new IllegalArgumentException("callback cannot be null");
		}
		Long id = Long.valueOf(0 - mCurrId.incrementAndGet());
		mAsyncCallbacks.put(id, cb);
		// TODO: if this fails (throws exception) then must remove id from mAsyncCallbacks
		addRequest(cmd, args, id.longValue(), null);
		return id;
	}

	@Override
	public boolean unregister(Object id) {
		return mAsyncCallbacks.remove(id) == null ? false : true;
	}

	/**
	 * Queue a command that will:
	 * 	 1) close the send thread after the command has been written; no more commands will be sent
	 *   2) close the recv thread after the command's response has been parsed and the callback has been invoked
	 * @param cmd  Command to run (ie, QUIT)
	 * @param args Command's parameters. Do not modify
	 * @param cb   Callback to invoke when response is received
	 */
	public void quit(String cmd, Iterator<Object> args, CallbackSF<Object,Object> cb) {
		if (mQuitCB1 != null) {
			throw new IllegalStateException();
		}
		mQuitCB1 = cb;
		addRequest(cmd, args, 0, mQuitCB2);
	}
}
