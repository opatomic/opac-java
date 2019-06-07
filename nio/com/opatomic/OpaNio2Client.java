/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.opatomic;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channels;
import java.nio.channels.CompletionHandler;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class OpaNio2Client implements OpaClient<Object,OpaRpcError> {
	private static final int RECVBUFFLEN = 1024 * 8;
	private static final int SENDBUFFLEN = 1024 * 8;

	private static AsynchronousChannelGroup CHGRP;
	static {
		try {
			CHGRP = AsynchronousChannelGroup.withThreadPool(OpaNioClient.EXSVC);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	private final AtomicLong mCurrId = new AtomicLong();
	private final AsynchronousSocketChannel mCh;
	private final Queue<CallbackSF<Object,OpaRpcError>> mMainCallbacks = new LinkedBlockingQueue<CallbackSF<Object,OpaRpcError>>();
	private final Map<Long,CallbackSF<Object,OpaRpcError>> mAsyncCallbacks = new ConcurrentHashMap<Long,CallbackSF<Object,OpaRpcError>>();
	private final OpaClientRecvState mRecvState = new OpaClientRecvState(mMainCallbacks, mAsyncCallbacks);
	private final ByteBuffer mRecvBuff = ByteBuffer.allocate(RECVBUFFLEN);
	private final RequestSerializer mReqSer;

	private OpaNio2Client(AsynchronousSocketChannel ch) {
		mCh = ch;
		mReqSer = new RequestSerializer(mMainCallbacks, mAsyncCallbacks, OpaNioClient.EXSVC, Channels.newOutputStream(ch), SENDBUFFLEN);
	}

	private void addRequest(String command, Iterator<Object> args, long id, CallbackSF<Object,OpaRpcError> cb) {
		mReqSer.sendRequest(command, args, id, cb);
	}

	@Override
	public void call(String cmd, Iterator<Object> args, CallbackSF<Object,OpaRpcError> cb) {
		addRequest(cmd, args, 0, cb);
	}

	@Override
	public void callA(String cmd, Iterator<Object> args, CallbackSF<Object,OpaRpcError> cb) {
		if (cb == null) {
			throw new IllegalArgumentException("callback cannot be null");
		}
		addRequest(cmd, args, mCurrId.incrementAndGet(), cb);
	}

	@Override
	public Object callAP(String cmd, Iterator<Object> args, CallbackSF<Object,OpaRpcError> cb) {
		if (cb == null) {
			throw new IllegalArgumentException("callback cannot be null");
		}
		Long id = Long.valueOf(0 - mCurrId.incrementAndGet());
		mAsyncCallbacks.put(id, cb);
		// TODO: if this fails (throws exception) then must remove persistent id
		addRequest(cmd, args, id.longValue(), null);
		return id;
	}

	@Override
	public boolean unregister(Object id) {
		return mAsyncCallbacks.remove(id) == null ? false : true;
	}

	public void close() {
		try {
			mCh.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static CompletionHandler<Integer,OpaNio2Client> READCH = new CompletionHandler<Integer,OpaNio2Client>() {
		@Override
		public void completed(Integer result, OpaNio2Client c) {
			int numRead = result.intValue();
			if (numRead < 0) {
				c.close();
			} else {
				c.mRecvState.onRecv(c.mRecvBuff.array(), c.mRecvBuff.arrayOffset(), numRead);
				c.mRecvBuff.clear();
			}
		}
		@Override
		public void failed(Throwable exc, OpaNio2Client c) {
			c.close();
		}
	};

	private static CompletionHandler<Void,OpaNio2Client> CONNECTCH = new CompletionHandler<Void,OpaNio2Client>() {
		@Override
		public void completed(Void result, OpaNio2Client c) {
			c.mCh.read(c.mRecvBuff, c, READCH);
		}
		@Override
		public void failed(Throwable exc, OpaNio2Client c) {
			c.close();
		}
	};

	private static OpaNio2Client connect(SocketAddress addr, AsynchronousChannelGroup acg) throws IOException {
		AsynchronousSocketChannel ch = AsynchronousSocketChannel.open(acg);
		OpaNio2Client c = new OpaNio2Client(ch);
		ch.connect(addr, c, CONNECTCH);
		return c;
	}

	/**
	 * Create a new client and connect to the specified address.
	 * @param addr address of Opatomic server
	 * @return new client
	 * @throws IOException
	 */
	public static OpaNio2Client connect(SocketAddress addr) throws IOException {
		return connect(addr, CHGRP);
	}
}
