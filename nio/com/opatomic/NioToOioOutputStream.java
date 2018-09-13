package com.opatomic;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

final class NioToOioOutputStream extends OutputStream  {
	private final OpaNioSelector mSelector;
	private final SocketChannel mChannel;
	private final OpaNioSelector.NioSelectionHandler mHandler;
	private boolean mWritable = false;

	NioToOioOutputStream(OpaNioSelector s, SocketChannel ch, OpaNioSelector.NioSelectionHandler h) {
		mSelector = s;
		mChannel = ch;
		mHandler = h;
	}

	private void waitUntilWritable() {
		while (!mWritable) {
			try {
				wait(0);
			} catch (InterruptedException e) {}
		}
	}

	public synchronized void setWritable() {
		mWritable = true;
		notifyAll();
	}

	@Override
	public synchronized void write(byte[] buff, int off, int len) throws IOException {
		while (len > 0) {
			waitUntilWritable();
			int numWritten = mChannel.write(ByteBuffer.wrap(buff, off, len));
			off += numWritten;
			len -= numWritten;
			if (numWritten == 0) {
				mWritable = false;
				mSelector.register(mChannel, mHandler, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
			}
		}
	}

	@Override
	public void write(int v) throws IOException {
		// writing 1 byte at a time is inefficient!
		throw new UnsupportedOperationException();
	}
}