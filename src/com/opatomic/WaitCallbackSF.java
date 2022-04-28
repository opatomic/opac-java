/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.opatomic;

import java.util.concurrent.TimeoutException;

/**
 * A callback that helps make a request synchronous by waiting for response.
 *
 * Example usage:
 * <pre>
 * {@code
 *   WaitCallbackSF<Object,Object> wcb = new WaitCallbackSF<Object,Object>();
 *   client.call("PING", null, wcb);
 *   wcb.waitIfNotDone();
 *   if (wcb.error != null) {
 *     // error occurred
 *   } else {
 *     // no error, result is stored in wcb.result
 *   }
 *   // can re-use wcb by calling reset()
 *   client.call("PING", null, wcb.reset());
 *   wcb.waitIfNotDone();
 *   // ... check wcb.error/wcb.result ...
 * }
 * </pre>
 * @param <R> Object type for result
 * @param <E> Object type for error
 */
public class WaitCallbackSF<R,E> implements CallbackSF<R,E> {
	private R mResult;
	private E mError;
	private boolean mIsDone;

	/**
	 * Get the result of the callback. Must call getError() first to determine whether an error occurred.
	 * @return result
	 * @throws IllegalStateException if the error/result is not available
	 * @throws IllegalStateException if the callback failed and an error exists
	 */
	public R getResult() {
		if (!mIsDone) {
			throw new IllegalStateException("result is not available yet");
		} else if (mError != null) {
			throw new IllegalStateException("callback failed; getResult() cannot be called");
		}
		return mResult;
	}

	/**
	 * Get the error of the callback. Returns null if an error did not occur.
	 * @return result
	 * @throws IllegalStateException if the error/result is not available
	 */
	public E getError() {
		if (!mIsDone) {
			throw new IllegalStateException("error is not available yet");
		}
		return mError;
	}

	@Override
	public synchronized void onSuccess(R result) {
		//if (Debug.ENABLE) Debug.debugAssert(!mIsDone, "Callback invoked multiple times");
		mResult = result;
		mIsDone = true;
		notifyAll();
	}

	@Override
	public synchronized void onFailure(E error) {
		//if (Debug.ENABLE) Debug.debugAssert(!mIsDone, "Callback invoked multiple times");
		mError = error;
		mIsDone = true;
		notifyAll();
	}

	/**
	 * Wait forever until a response is received.
	 * @throws InterruptedException
	 */
	public synchronized void waitIfNotDone() throws InterruptedException {
		// loop because Object.wait() may wake up randomly (spurious wakeup)
		while (!mIsDone) {
			wait(0);
		}
	}

	/**
	 * Wait until a response is received or a timeout is exceeded
	 * @param millis max milliseconds to wait for response
	 * @throws InterruptedException
	 * @throws TimeoutException
	 */
	public synchronized void waitIfNotDone(long millis) throws InterruptedException, TimeoutException {
		if (millis == 0) {
			waitIfNotDone();
		} else {
			long endTime = System.currentTimeMillis() + millis;
			// loop because Object.wait() may wake up randomly (spurious wakeup)
			while (!mIsDone) {
				wait(millis);
				if (mIsDone) {
					return;
				}
				long currTime = System.currentTimeMillis();
				if (currTime >= endTime) {
					throw new TimeoutException("wait() timed out");
				}
				millis = endTime - currTime;
			}
		}
	}

	/**
	 * Clear the callback's state so it can be used again
	 * @return this callback
	 */
	public WaitCallbackSF<R,E> reset() {
		mIsDone = false;
		mResult = null;
		mError = null;
		return this;
	}
}
