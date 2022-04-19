/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.opatomic;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

final class OpaNioSelector implements Runnable {
	public interface NioSelectionHandler {
		public void handle(SelectableChannel selch, SelectionKey key, int readyOps) throws IOException;
	}

	private final Selector mSelector;
	private Thread mLoopThread = null;

	private Queue<Runnable> mWork = new LinkedBlockingQueue<Runnable>();

	private long mSelectTimeout;
	private int mMaxKeys = 5;
	private boolean mClosed = false;

	OpaNioSelector(long timeoutMillis) throws IOException {
		mSelectTimeout = timeoutMillis;
		mSelector = Selector.open();
	}

	private void registerInternal(SelectableChannel sc, NioSelectionHandler h, int ops) {
		try {
			sc.register(mSelector, ops, h);
		} catch (ClosedChannelException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	// can be called at any time from any thread
	public void register(final SelectableChannel sc, final NioSelectionHandler h, final int ops) {
		if (sc == null || h == null) {
			throw new IllegalArgumentException();
		}
		if (Thread.currentThread() == mLoopThread) {
			registerInternal(sc, h, ops);
		} else {
			mWork.add(new Runnable() {
				@Override
				public void run() {
					registerInternal(sc, h, ops);
				}
			});
			mSelector.wakeup();
		}
	}

	public void run() {
		mLoopThread = Thread.currentThread();
		while (true) {
			try {
				// Ensure we're clearing/closing unused channels
				if (OpaDef.DEBUG) {
					int size = mSelector.keys().size();
					if (size > mMaxKeys) {
						mMaxKeys = size;
						OpaDef.log("Selector highwater = " + Integer.toString(size));
					}
				}

				if (mSelector.select(mSelectTimeout) > 0) {
					Set<SelectionKey> keys = mSelector.selectedKeys();
					Iterator<SelectionKey> i = keys.iterator();
					while (i.hasNext()) {
						SelectionKey key = i.next();
						NioSelectionHandler h = (NioSelectionHandler) key.attachment();
						if (!key.isValid()) {
							continue;
						}
						SelectableChannel sc = key.channel();
						h.handle(sc, key, key.readyOps());
					}
					keys.clear();
				}

				while (true) {
					Runnable r = mWork.poll();
					if (r == null) {
						break;
					}
					r.run();
				}

				// Ensure we're clearing/closing unused channels
				//if (OpaDef.DEBUG && System.currentTimeMillis() > mDebugTime && mSelector.keys().size() > 10) {
				//	OpaDef.log("Warning: selector keys registered = " + Integer.toString(mSelector.keys().size()));
				//	mDebugTime = System.currentTimeMillis() + 1000;
				//}

			} catch (Exception e) {
				e.printStackTrace();
				if (mClosed) {
					break;
				}
				try {
					Thread.sleep(100);
				} catch (Exception e2) {}
			}
		}
	}
}
