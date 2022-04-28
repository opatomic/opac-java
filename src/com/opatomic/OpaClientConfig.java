package com.opatomic;

public class OpaClientConfig {
	public interface RawResponseHandler {
		void handle(Object id, Object result, Object err);
	}
	public interface ExceptionHandler {
		void handle(Throwable e, Object context);
	}

	public static final OpaClientConfig DEFAULT_CFG;
	static {
		DEFAULT_CFG = new OpaClientConfig();
		DEFAULT_CFG.unknownIdHandler = new RawResponseHandler() {
			@Override
			public void handle(Object id, Object result, Object err) {
				System.err.println("Unknown callback id " + OpaUtils.stringify(id));
			}
		};
		DEFAULT_CFG.uncaughtExceptionHandler = new ExceptionHandler() {
			@Override
			public void handle(Throwable e, Object context) {
				e.printStackTrace();
			}
		};
	}

	/**
	 * Size of the recv/parse buffer in bytes.
	 */
	public int recvBuffLen = 1024 * 4;

	/**
	 * Size of the serializer buffer in bytes.
	 */
	public int sendBuffLen = 1024 * 4;

	/**
	 * Max length of the send queue. When the length is reached, callers will block until a request
	 * has been removed from the send queue to be serialized. This is a form of back-pressure.
	 */
	public int sendQueueLen = 1024;

	/**
	 * Callback to invoke when a response is received without a registered callback.
	 */
	public RawResponseHandler unknownIdHandler;

	/**
	 * Callback to invoke when an uncaught exception occurs.
	 */
	public ExceptionHandler uncaughtExceptionHandler;

	/**
	 * Callback to invoke when an internal error occurs in the client. Examples could include parse
	 * errors or serialization errors.
	 */
	public ExceptionHandler clientErrorHandler;
}
