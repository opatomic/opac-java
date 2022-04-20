package com.opatomic;

public class OpaClientConfig {
	public static final OpaClientConfig DEFAULT_CFG;
	static {
		DEFAULT_CFG = new OpaClientConfig();
		DEFAULT_CFG.unknownIdHandler = new OpaRawResponseHandler() {
			@Override
			public void handle(Object id, Object result, Object err) {
				System.err.println("Unknown callback id " + OpaUtils.stringify(id));
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
	public int sendQueueLen = Integer.MAX_VALUE;

	/**
	 * Callback to invoke when a response is received without a registered callback.
	 */
	public OpaRawResponseHandler unknownIdHandler;
}
