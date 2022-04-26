/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.opatomic;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

final class OpaNio2CopyOutputStream extends OutputStream {
	private static final int INIT_BUFF_LEN = 1024 * 2;
	private static final int MAX_BUFF_LEN = 1024 * 512;

	private final OpaNio2Client mClient;
	private final AsynchronousByteChannel mChan;

	private boolean mWriteOutstanding = false;
	private boolean mWriting = false;
	private ByteBuffer mBuff1 = ByteBuffer.allocate(INIT_BUFF_LEN);
	private ByteBuffer mBuff2 = ByteBuffer.allocate(INIT_BUFF_LEN);

	OpaNio2CopyOutputStream(OpaNio2Client c, AsynchronousByteChannel ch) {
		mClient = c;
		mChan = ch;
	}

	private static CompletionHandler<Integer,OpaNio2CopyOutputStream> WRITECH = new CompletionHandler<Integer,OpaNio2CopyOutputStream>() {
		@Override
		public void completed(Integer result, OpaNio2CopyOutputStream o) {
			o.writeComplete();
		}
		@Override
		public void failed(Throwable exc, OpaNio2CopyOutputStream o) {
			o.close();
		}
	};

	private static ByteBuffer clearBuff(ByteBuffer bb) {
		if (bb.capacity() > MAX_BUFF_LEN) {
			return ByteBuffer.allocate(INIT_BUFF_LEN);
		} else {
			// TODO: zero buffer contents?
			bb.clear();
			return bb;
		}
	}

	private static ByteBuffer appendBB(ByteBuffer bb, byte[] data, int off, int len) {
		if (bb.remaining() < len) {
			int cap = bb.capacity();
			int newSize = Math.max(cap + cap/2, bb.position() + len);
			ByteBuffer newbb = ByteBuffer.allocate(newSize);
			bb.flip();
			newbb.put(bb);
			bb = newbb;
		}
		bb.put(data, off, len);
		return bb;
	}

	private synchronized void writeComplete() {
		if (mBuff1.hasRemaining()) {
			mChan.write(mBuff1, this, WRITECH);
		} else {
			mBuff1 = clearBuff(mBuff1);
			if (mBuff2.position() > 0) {
				ByteBuffer tmp = mBuff1;
				mBuff1 = mBuff2;
				mBuff2 = tmp;
				mBuff1.flip();
				mChan.write(mBuff1, this, WRITECH);
			} else {
				mWriteOutstanding = false;
				// write completion handler can be called immediately; detect this to prevent recursion
				if (!mWriting) {
					mClient.flush();
				}
			}
		}
	}

	boolean isWriteOutstanding() {
		assert Thread.holdsLock(this);
		return mWriteOutstanding;
	}

	@Override
	public void close() {
		mClient.close();
	}

	@Override
	public void write(byte[] buff, int off, int len) throws IOException {
		assert Thread.holdsLock(this);
		if (!mWriteOutstanding) {
			mBuff1 = appendBB(mBuff1, buff, off, len);
			mWriteOutstanding = true;
			mWriting = true;
			mBuff1.flip();
			mChan.write(mBuff1, this, WRITECH);
			mWriting = false;
		} else {
			mBuff2 = appendBB(mBuff2, buff, off, len);
		}
	}

	@Override
	public void write(byte[] buff) throws IOException {
		write(buff, 0, buff.length);
	}

	@Override
	public void write(int arg0) throws IOException {
		// writing 1 byte at a time is inefficient!
		throw new UnsupportedOperationException();
	}
}

public class OpaNio2Client implements OpaClient {
	private final OpaClientConfig mConfig;
	private final AtomicLong mCurrId = new AtomicLong();
	private final Queue<CallbackSF<Object,OpaRpcError>> mMainCallbacks = new ConcurrentLinkedQueue<CallbackSF<Object,OpaRpcError>>();
	private final Map<Object,CallbackSF<Object,OpaRpcError>> mAsyncCallbacks = new ConcurrentHashMap<Object,CallbackSF<Object,OpaRpcError>>();
	private final OpaClientRecvState mRecvState;
	private final ByteBuffer mRecvBuff;
	private final OpaNio2CopyOutputStream mOut;
	private final OpaSerializer mSerializer;
	private final Queue<Request> mSerializeQueue = new LinkedList<Request>();
	private final AsynchronousByteChannel mChan;
	private boolean mAutoFlush = true;

	/**
	 * Create a new client that uses the Java NIO2 API.
	 * @param ch   The channel to use. Must be connected already.
	 * @param cfg  Client options. See OpaClientConfig for details.
	 */
	public OpaNio2Client(AsynchronousByteChannel ch, OpaClientConfig cfg) {
		mChan = ch;
		mConfig = cfg;
		mRecvState = new OpaClientRecvState(mMainCallbacks, mAsyncCallbacks, cfg);
		mRecvBuff = ByteBuffer.allocate(cfg.recvBuffLen);
		mOut = new OpaNio2CopyOutputStream(this, ch);
		mSerializer = new OpaSerializer(mOut, cfg.sendBuffLen);
		mChan.read(mRecvBuff, this, READCH);
	}

	public boolean setAutoFlush(boolean onOrOff) {
		boolean prevVal = mAutoFlush;
		mAutoFlush = onOrOff;
		return prevVal;
	}

	public void flush() {
		try {
			synchronized (mOut) {
				while (!mOut.isWriteOutstanding()) {
					Request r = mSerializeQueue.poll();
					if (r == null) {
						// queue has been drained
						mSerializer.flush();
						break;
					}
					OpaStreamClient.writeRequest(mSerializer, r.command, r.args, r.asyncId);
				}
			}
		} catch (Exception e) {
			close();
		}
	}

	private void sendRequest(CharSequence cmd, Iterator<?> args, Object id, CallbackSF<Object,OpaRpcError> cb) {
		try {
			synchronized (mOut) {
				if (id == null) {
					// note: adding to mMainCallbacks must be inside synchronized lock block to make sure the request is serialized at same time
					//       it was added to mMainCallbacks. Otherwise, another request could be serialized in between the following things
					//       happening: (1) this request added to queue and (2) this request being serialized (or added to serialize queue).
					mMainCallbacks.add(cb);
				}
				if (mOut.isWriteOutstanding()) {
					mSerializeQueue.add(new Request(cmd, args, id, cb));
				} else {
					OpaStreamClient.writeRequest(mSerializer, cmd, args, id);
					if (mAutoFlush && !mOut.isWriteOutstanding()) {
						mSerializer.flush();
					}
				}
			}
		} catch (Exception e) {
			close();
		}
	}

	@Override
	public void call(CharSequence cmd, Iterator<?> args, CallbackSF<Object,OpaRpcError> cb) {
		sendRequest(cmd, args, cb == null ? Boolean.FALSE : null, cb);
	}

	@Override
	public void callA(CharSequence cmd, Iterator<?> args, CallbackSF<Object,OpaRpcError> cb) {
		if (cb == null) {
			throw new IllegalArgumentException("callback cannot be null");
		}
		Long id = mCurrId.incrementAndGet();
		mAsyncCallbacks.put(id, cb);
		boolean removeCB = true;
		try {
			sendRequest(cmd, args, id, cb);
			removeCB = false;
		} finally {
			if (removeCB) {
				mAsyncCallbacks.remove(id);
			}
		}
	}

	@Override
	public CallbackSF<Object, OpaRpcError> registerCB(Object id, CallbackSF<Object, OpaRpcError> cb) {
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
		sendRequest(cmd, args, id, null);
	}

	public void close() {
		try {
			mChan.close();
		} catch (IOException e) {
		}
		OpaClientUtils.respondWithClosedErr(mConfig, mMainCallbacks, mAsyncCallbacks);
	}

	private static CompletionHandler<Integer,OpaNio2Client> READCH = new CompletionHandler<Integer,OpaNio2Client>() {
		@Override
		public void completed(Integer result, OpaNio2Client c) {
			try {
				int numRead = result.intValue();
				if (numRead < 0) {
					c.close();
				} else {
					c.mRecvState.onRecv(c.mRecvBuff.array(), c.mRecvBuff.arrayOffset(), numRead);
					c.mRecvBuff.clear();
					c.mChan.read(c.mRecvBuff, c, READCH);
				}
			} catch (Exception e) {
				c.close();
			}
		}
		@Override
		public void failed(Throwable exc, OpaNio2Client c) {
			c.close();
		}
	};




	private final static class DaemonThreadFactory implements ThreadFactory {
		public Thread newThread(Runnable target) {
			Thread t = new Thread(target);
			t.setDaemon(true);
			return t;
		}
	}

	private static final Object LOCK = new Object();
	private static AsynchronousChannelGroup CHGRP = null;

	private static void startService() throws IOException {
		synchronized (LOCK) {
			if (CHGRP == null) {
				int cores = Runtime.getRuntime().availableProcessors();
				ExecutorService svc = new ThreadPoolExecutor(cores, cores, 15, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new DaemonThreadFactory());
				CHGRP = AsynchronousChannelGroup.withThreadPool(svc);
			}
		}
	}

	public static OpaNio2Client connect(SocketAddress addr, AsynchronousSocketChannel ch, long timeout, TimeUnit unit, OpaClientConfig cfg) throws IOException, InterruptedException, ExecutionException, TimeoutException {
		Future<Void> f = ch.connect(addr);
		f.get(timeout, unit);
		ch.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);
		//ch.setOption(StandardSocketOptions.SO_SNDBUF, 1024);
		return new OpaNio2Client(ch, cfg);
	}

	/**
	 * Create a new client and connect to the specified address (blocks until connect is complete or timeout occurs). All clients will use a shared internal
	 * AsynchronousChannelGroup that uses a shared internal ExecutorService.
	 * @param addr    Address of Opatomic server
	 * @param timeout The maximum time to wait
	 * @param unit    The time unit of the timeout argument
	 * @param cfg     Client options
	 * @return the new client
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	public static OpaNio2Client connect(SocketAddress addr, long timeout, TimeUnit unit, OpaClientConfig cfg) throws IOException, InterruptedException, ExecutionException, TimeoutException {
		startService();
		return connect(addr, AsynchronousSocketChannel.open(CHGRP), timeout, unit, cfg);
	}

	public static <A> void connect(SocketAddress addr, final AsynchronousSocketChannel ch, OpaClientConfig cfg, final A attachment, final CompletionHandler<OpaNio2Client,? super A> cb) throws IOException, InterruptedException, ExecutionException, TimeoutException {
		ch.connect(addr, null, new CompletionHandler<Void,A>() {
			@Override
			public void completed(Void unused1, Object unused2) {
				try {
					ch.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);
				} catch (IOException e) {
				}
				cb.completed(new OpaNio2Client(ch, cfg), attachment);
			}
			@Override
			public void failed(Throwable exc, Object unused2) {
				cb.failed(exc, attachment);
			}
		});
	}
}
