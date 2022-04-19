/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.opatomic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

final class OpaNioBufferedOutputStream extends OutputStream {
	private final OpaNioSelector mSelector;
	private final SocketChannel mChannel;
	private final OpaNioSelector.NioSelectionHandler mHandler;
	private final ByteArrayOutputStream mBuff = new ByteArrayOutputStream();
	private boolean mWritable = false;

	OpaNioBufferedOutputStream(OpaNioSelector s, SocketChannel ch, OpaNioSelector.NioSelectionHandler h) {
		mSelector = s;
		mChannel = ch;
		mHandler = h;
	}

	public boolean isWritable() {
		return mWritable;
	}

	public synchronized void onWritable() throws IOException {
		mWritable = true;
		byte[] bytesToWrite = mBuff.toByteArray();
		mBuff.reset();
		write(bytesToWrite);
	}

	@Override
	public synchronized void write(byte[] buff, int off, int len) throws IOException {
		if (mWritable) {
			while (len > 0) {
				int numWritten = mChannel.write(ByteBuffer.wrap(buff, off, len));
				off += numWritten;
				len -= numWritten;
				if (numWritten == 0) {
					// cannot write anymore
					mWritable = false;
					// register to know when channel is writable
					mSelector.register(mChannel, mHandler, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
					// buffer writes into memory
					mBuff.write(buff, off, len);
					return;
				}
			}
		} else {
			mBuff.write(buff, off, len);
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

public class OpaNioClient implements OpaClient {
	private static final int RECVREADITS = 1;

	private final OpaNioSelector.NioSelectionHandler mHandler = new OpaNioSelector.NioSelectionHandler() {
		@Override
		public void handle(SelectableChannel selch, SelectionKey key, int readyOps) throws IOException {
			SocketChannel sc = (SocketChannel) selch;
			if ((readyOps & SelectionKey.OP_READ) != 0) {
				for (int i = 0; i < RECVREADITS; ++i) {
					int numRead;
					try {
						numRead = sc.read(mRecvBuff);
					} catch (Exception e) {
						numRead = -1;
					}
					if (numRead <= 0) {
						if (numRead < 0) {
							close();
							return;
						}
						break;
					}
					mRecvState.onRecv(mRecvBuff.array(), mRecvBuff.arrayOffset(), numRead);
					mRecvBuff.clear();
				}
			}
			if ((readyOps & SelectionKey.OP_WRITE) != 0) {
				mSelector.register(selch, this, SelectionKey.OP_READ);
				onWritable();
			}
			if ((readyOps & SelectionKey.OP_CONNECT) != 0) {
				sc.finishConnect();
			}
		}
	};

	private final AtomicLong mCurrId = new AtomicLong();
	private final Queue<CallbackSF<Object,OpaRpcError>> mMainCallbacks = new ConcurrentLinkedQueue<CallbackSF<Object,OpaRpcError>>();
	private final Map<Object,CallbackSF<Object,OpaRpcError>> mAsyncCallbacks = new ConcurrentHashMap<Object,CallbackSF<Object,OpaRpcError>>();
	private final OpaClientRecvState mRecvState = new OpaClientRecvState(mMainCallbacks, mAsyncCallbacks);
	private final ByteBuffer mRecvBuff;
	private final OpaNioSelector mSelector;
	private final SocketChannel mChan;

	private final OpaNioBufferedOutputStream mOut;
	private final OpaSerializer mSerializer;
	private final Queue<Request> mSerializeQueue = new ConcurrentLinkedQueue<Request>();
	private boolean mUseQueue = false;
	private boolean mAutoFlush = true;

	OpaNioClient(SocketChannel ch, OpaNioSelector sel, int recvBuffLen, int sendBuffLen) {
		mRecvBuff = ByteBuffer.allocate(recvBuffLen);
		mOut = new OpaNioBufferedOutputStream(sel, ch, mHandler);
		mSerializer = new OpaSerializer(mOut, sendBuffLen);
		mSelector = sel;
		mChan = ch;
		sel.register(ch, mHandler, SelectionKey.OP_READ | SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT);
	}

	public synchronized void close() {
		try {
			mChan.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		OpaStreamClient.respondWithClosedErr(mMainCallbacks, mAsyncCallbacks);
	}

	public boolean setAutoFlush(boolean onOrOff) {
		boolean prevVal = mAutoFlush;
		mAutoFlush = onOrOff;
		return prevVal;
	}

	private void flushInternal() throws IOException {
		if (mOut.isWritable()) {
			mSerializer.flush();
		}
		if (!mOut.isWritable()) {
			mUseQueue = true;
		}
	}

	public synchronized void flush() {
		try {
			flushInternal();
		} catch (Exception e) {
			close();
		}
	}

	private synchronized void onWritable() {
		try {
			mOut.onWritable();
			while (mOut.isWritable()) {
				Request r = mSerializeQueue.poll();
				if (r == null) {
					// queue has been drained
					mSerializer.flush();
					if (mOut.isWritable()) {
						mUseQueue = false;
					}
					break;
				}
				OpaStreamClient.writeRequest(mSerializer, r.command, r.args, r.asyncId);
			}
		} catch (Exception e) {
			close();
		}
	}

	private synchronized void addRequest(CharSequence command, Iterator<?> args, Object id, CallbackSF<Object,OpaRpcError> cb) {
		try {
			if (id == null) {
				mMainCallbacks.add(cb);
			}
			if (mUseQueue) {
				mSerializeQueue.add(new Request(command, args, id, cb));
			} else {
				OpaStreamClient.writeRequest(mSerializer, command, args, id);
				if (mAutoFlush) {
					flushInternal();
				}
			}
		} catch (Exception e) {
			close();
		}
	}

	@Override
	public void call(CharSequence cmd, Iterator<?> args, CallbackSF<Object,OpaRpcError> cb) {
		addRequest(cmd, args, cb == null ? Boolean.FALSE : null, cb);
	}

	@Override
	public void callA(CharSequence cmd, Iterator<?> args, CallbackSF<Object,OpaRpcError> cb) {
		if (cb == null) {
			throw new IllegalArgumentException("callback cannot be null");
		}
		Long id = mCurrId.incrementAndGet();
		mAsyncCallbacks.put(id, cb);
		addRequest(cmd, args, id, cb);
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
		addRequest(cmd, args, id, null);
	}




	private static final Object LOCK = new Object();
	private static OpaNioSelector SELECTOR;
	private static Thread SELECTOR_THREAD = null;

	private static void startService() throws IOException {
		synchronized (LOCK) {
			if (SELECTOR_THREAD == null || !SELECTOR_THREAD.isAlive()) {
				SELECTOR = new OpaNioSelector(1000);
				SELECTOR_THREAD = OpaUtils.startDaemonThread(SELECTOR, "OpaNioSelector");
			}
		}
	}

	/**
	 * Create a new client and connect to the specified address.
	 * @param addr address of Opatomic server
	 * @return new client
	 * @throws IOException
	 */
	public static OpaNioClient connect(SocketAddress addr, int timeoutMillis) throws IOException {
		startService();
		SocketChannel sc = SocketChannel.open();
		sc.configureBlocking(true);
		sc.socket().connect(addr, timeoutMillis);
		sc.configureBlocking(false);
		sc.socket().setTcpNoDelay(true);
		//sc.socket().setSendBufferSize(1024);
		return new OpaNioClient(sc, SELECTOR, 1024 * 4, 1024 * 4);
	}
}
