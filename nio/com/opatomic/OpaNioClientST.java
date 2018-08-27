package com.opatomic;

import java.io.IOException;
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

public class OpaNioClientST implements OpaClient<Object,Object> {
	private static final int RECVREADITS = 1;
	private static final int RECVBUFFLEN = 1024 * 8;
	private static final int SENDBUFFLEN = 1024 * 8;

	private static final OpaNioSelector SELECTOR = new OpaNioSelector();
	private static final ByteBuffer RECVBUF = ByteBuffer.allocate(RECVBUFFLEN);

	static {
		OpaUtils.startDaemonThread(SELECTOR, "OpatomicNioSelector");
	}

	private long mCurrId = 0;
	private final Queue<CallbackSF<Object,Object>> mMainCallbacks = new ConcurrentLinkedQueue<CallbackSF<Object,Object>>();
	private final Map<Long,CallbackSF<Object,Object>> mAsyncCallbacks = new ConcurrentHashMap<Long,CallbackSF<Object,Object>>();
	private final OpaClientRecvState mRecvState = new OpaClientRecvState(mMainCallbacks, mAsyncCallbacks);
	private final OpaSerializer mSerializer;
	private final NioToOioOutputStream mOut;

	private final OpaNioSelector.NioSelectionHandler mHandler = new OpaNioSelector.NioSelectionHandler() {
		@Override
		public void handle(SelectableChannel selch, SelectionKey key, int readyOps) throws IOException {
			SocketChannel sc = (SocketChannel) selch;
			if ((readyOps & SelectionKey.OP_READ) != 0) {
				for (int i = 0; i < RECVREADITS; ++i) {
					int numRead = sc.read(RECVBUF);
					if (numRead <= 0) {
						if (numRead < 0) {
							selch.close();
						}
						break;
					}
					mRecvState.onRecv(RECVBUF.array(), RECVBUF.arrayOffset(), numRead);
					RECVBUF.clear();
				}
			}
			if ((readyOps & SelectionKey.OP_WRITE) != 0) {
				SELECTOR.register(selch, this, SelectionKey.OP_READ);
				mOut.setWritable();
			}
			if ((readyOps & SelectionKey.OP_CONNECT) != 0) {
				sc.finishConnect();
				mOut.setWritable();
			}
		}
	};

	OpaNioClientST(SocketChannel ch) {
		mOut = new NioToOioOutputStream(SELECTOR, ch, mHandler);
		mSerializer = new OpaSerializer(mOut, SENDBUFFLEN);
		SELECTOR.register(ch, mHandler, SelectionKey.OP_READ | SelectionKey.OP_CONNECT);
	}

	private void sendRequest(String command, Iterator<Object> args, long id, CallbackSF<Object,Object> cb) {
		if (cb != null) {
			if (id != 0) {
				mAsyncCallbacks.put(id, cb);
			} else {
				mMainCallbacks.add(cb);
			}
		}

		try {
			// TODO: design a serializer that can pause at any point if a write would block
			OpaStreamClient.writeRequest(mSerializer, command, args, id, cb == null);
			mSerializer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void call(String cmd, Iterator<Object> args, CallbackSF<Object, Object> cb) {
		sendRequest(cmd, args, 0, cb);
	}

	@Override
	public synchronized void callA(String cmd, Iterator<Object> args, CallbackSF<Object, Object> cb) {
		if (cb == null) {
			throw new IllegalArgumentException("callback cannot be null");
		}
		sendRequest(cmd, args, ++mCurrId, cb);
	}

	@Override
	public synchronized Object callAP(String cmd, Iterator<Object> args, CallbackSF<Object, Object> cb) {
		if (cb == null) {
			throw new IllegalArgumentException("callback cannot be null");
		}
		Long id = Long.valueOf(0 - (++mCurrId));
		mAsyncCallbacks.put(id, cb);
		// TODO: if this fails (throws exception) then must remove persistent id
		sendRequest(cmd, args, id.longValue(), null);
		return id;
	}

	@Override
	public synchronized boolean unregister(Object id) {
		return mAsyncCallbacks.remove(id) == null ? false : true;
	}

	/**
	 * Create a new client and connect to the specified address.
	 * @param addr address of Opatomic server 
	 * @return new client
	 * @throws IOException
	 */
	public static OpaNioClientST connect(SocketAddress addr) throws IOException {
		SocketChannel sc = SocketChannel.open();
		sc.configureBlocking(false);
		sc.connect(addr);
		return new OpaNioClientST(sc);
	}
}
