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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


final class DaemonThreadFactory implements ThreadFactory {
	public Thread newThread(Runnable arg0) {
		Thread t = new Thread(arg0);
		t.setDaemon(true);
		return t;
	}
}

public class OpaNioClient implements OpaClient<Object,OpaRpcError> {
	private static final int RECVREADITS = 1;
	private static final int RECVBUFFLEN = 1024 * 8;
	private static final int SENDBUFFLEN = 1024 * 8;

	private static final OpaNioSelector SELECTOR = new OpaNioSelector();
	private static final ByteBuffer RECVBUF = ByteBuffer.allocate(RECVBUFFLEN);

	// note: this executor service should be configurable! core thread, max threads, thread idle timeout
	private static final ExecutorService EXSVC = new ThreadPoolExecutor(4, 4, 15, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new DaemonThreadFactory());

	static {
		OpaUtils.startDaemonThread(SELECTOR, "OpatomicNioSelector");
	}


	private final Runnable mRunSerialize = new Runnable() {
		@Override
		public void run() {
			try {
				serializeRequests();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	};

	private final OpaNioSelector.NioSelectionHandler mHandler = new OpaNioSelector.NioSelectionHandler() {
		@Override
		public void handle(SelectableChannel selch, SelectionKey key, int readyOps) throws IOException {
			SocketChannel sc = (SocketChannel) selch;
			if ((readyOps & SelectionKey.OP_READ) != 0) {
				for (int i = 0; i < RECVREADITS; ++i) {
					int numRead;
					try {
						numRead = sc.read(RECVBUF);
					} catch (Exception e) {
						numRead = -1;
					}
					if (numRead <= 0) {
						if (numRead < 0) {
							selch.close();
							// call setWritable() in case writer thread is waiting
							mOut.setWritable();
							return;
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

	private final AtomicLong mCurrId = new AtomicLong();
	private final AtomicInteger mRequestQLen = new AtomicInteger(-2);
	private final OpaSerializer mSerializer;
	private final Queue<Request> mSerializeQueue = new LinkedBlockingQueue<Request>();
	private final Queue<CallbackSF<Object,OpaRpcError>> mMainCallbacks = new LinkedBlockingQueue<CallbackSF<Object,OpaRpcError>>();
	private final Map<Long,CallbackSF<Object,OpaRpcError>> mAsyncCallbacks = new ConcurrentHashMap<Long,CallbackSF<Object,OpaRpcError>>();
	private final OpaClientRecvState mRecvState = new OpaClientRecvState(mMainCallbacks, mAsyncCallbacks);
	private final NioToOioOutputStream mOut;

	OpaNioClient(SocketChannel ch) {
		mOut = new NioToOioOutputStream(SELECTOR, ch, mHandler);
		mSerializer = new OpaSerializer(mOut, SENDBUFFLEN);
		SELECTOR.register(ch, mHandler, SelectionKey.OP_READ | SelectionKey.OP_CONNECT);
	}

	private void sendRequest(Request r) throws IOException {
		if (r.cb != null) {
			if (r.id != 0) {
				mAsyncCallbacks.put(r.id, r.cb);
			} else {
				mMainCallbacks.add(r.cb);
			}
		}

		// TODO: design a serializer that can pause at any point if a write would block
		//OpaStreamClient.writeRequest(r, mSerializer);
		OpaStreamClient.writeRequest(mSerializer, r.command, r.args, r.id, r.cb == null);
	}

	private void serializeRequests() throws IOException {
		while (true) {
			Request r;
			while (true) {
				if ((r = mSerializeQueue.poll()) != null) {
					break;
				}
				Thread.yield();
			}
			sendRequest(r);

			int len = mRequestQLen.decrementAndGet();
			if (len < 0) {
				mSerializer.flush();
				len = mRequestQLen.decrementAndGet();
				if (len == -2) {
					return;
				}
				mRequestQLen.incrementAndGet();
			}
		}
	}

	private void addRequest(String command, Iterator<Object> args, long id, CallbackSF<Object,OpaRpcError> cb) {
		int len = mRequestQLen.getAndIncrement();
		mSerializeQueue.add(new Request(command, args, id, cb));
		if (len == -2) {
			mRequestQLen.getAndIncrement();
			EXSVC.execute(mRunSerialize);
		}
	}

	// TODO: must handle unexpected connection closing. threads might be waiting on response - need to invoke failure callback
	// TODO: call methods should check whether client is closed (before adding to serialize queue); if so then throw exception!

	@Override
	public void call(String cmd, Iterator<Object> args, CallbackSF<Object,OpaRpcError> cb) {
		//if (mQuitCB1 != null) {
		//	throw new IllegalStateException();
		//}
		addRequest(cmd, args, 0, cb);
	}

	@Override
	public void callA(String cmd, Iterator<Object> args, CallbackSF<Object,OpaRpcError> cb) {
		//if (mQuitCB1 != null) {
		//	throw new IllegalStateException();
		//}
		if (cb == null) {
			throw new IllegalArgumentException("callback cannot be null");
		}
		addRequest(cmd, args, mCurrId.incrementAndGet(), cb);
	}

	@Override
	public Object callAP(String cmd, Iterator<Object> args, CallbackSF<Object,OpaRpcError> cb) {
		//if (mQuitCB1 != null) {
		//	throw new IllegalStateException();
		//}
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

	/**
	 * Create a new client and connect to the specified address.
	 * @param addr address of Opatomic server
	 * @return new client
	 * @throws IOException
	 */
	public static OpaNioClient connect(SocketAddress addr) throws IOException {
		SocketChannel sc = SocketChannel.open();
		sc.configureBlocking(false);
		sc.connect(addr);
		return new OpaNioClient(sc);
	}
}
