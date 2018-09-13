package com.opatomic;

/**
 * Callback used to indicate success or failure
 *
 * @param <R> Success result type
 * @param <E> Error type
 */
public interface CallbackSF<R,E> {
	/**
	 * Invoked when success response is received
	 * @param result The successful response received
	 */
	public void onSuccess(R result);

	/**
	 * Invoked when error response is received
	 * @param error The error response received
	 */
	public void onFailure(E error);
}
