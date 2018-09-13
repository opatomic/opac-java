package com.opatomic;

import java.util.Arrays;

public class OpaRpcError {
	public final int code;
	public final String msg;
	public final Object data;

	public OpaRpcError(int code, String msg, Object data) {
		this.code = code;
		this.msg = msg;
		this.data = data;
	}

	public OpaRpcError(int code, String msg) {
		this(code, msg, null);
	}

	public OpaRpcError(int code) {
		this(code, null);
	}

	public String toString() {
		if (data != null) {
			return OpaUtils.stringify(Arrays.asList(code, msg, data));
		} else if (msg != null) {
			return OpaUtils.stringify(Arrays.asList(code, msg));
		}
		return Integer.toString(code);
	}
}
