/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.opatomic;

import java.util.List;
import java.util.Map;
import java.util.Queue;

class OpaClientRecvState {
	private final Queue<CallbackSF<Object,OpaRpcError>> mMainCallbacks;
	private final Map<Long,CallbackSF<Object,OpaRpcError>> mAsyncCallbacks;

	private final OpaPartialParser.Buff mBuff = new OpaPartialParser.Buff();
	private final OpaPartialParser mParser = new OpaPartialParser();

	//private long mNumRecv;

	public OpaClientRecvState(Queue<CallbackSF<Object,OpaRpcError>> maincbs, Map<Long,CallbackSF<Object,OpaRpcError>> asynccbs) {
		mMainCallbacks = maincbs;
		mAsyncCallbacks = asynccbs;
	}

	private int getErrorCode(Object codeObj) {
		long code = ((Long) codeObj).longValue();
		if (code > Integer.MAX_VALUE || code < Integer.MIN_VALUE || code == 0) {
			throw new RuntimeException("invalid error code: " + Long.toString(code));
		}
		return (int) code;
	}

	private void handleResponse(Object result, Object err, Object id) {
		CallbackSF<Object,OpaRpcError> cb;
		if (id != null) {
			if (!(id instanceof Long)) {
				//throw new RuntimeException("response id is not a Long");
				System.err.println("response id is not a Long: " + OpaUtils.stringify(id));
				return;
			}
			cb = ((Long)id).longValue() < 0 ? mAsyncCallbacks.get(id) : mAsyncCallbacks.remove(id);
			if (cb == null) {
				//throw new RuntimeException("Unknown callback id " + OpaUtils.stringify(id));
				System.err.println("Unknown callback id " + OpaUtils.stringify(id));
				return;
			}
		} else {
			cb = mMainCallbacks.remove();
		}

		OpaRpcError err2 = null;
		if (err != null) {
			if (err instanceof List) {
				List<?> lerr = (List<?>) err;
				if (lerr.size() < 2 || lerr.size() > 3) {
					throw new RuntimeException("error is an array of wrong size: " + lerr.size());
				}
				err2 = new OpaRpcError(getErrorCode(lerr.get(0)), (String) lerr.get(1), lerr.size() >= 3 ? lerr.get(2) : null);
			} else if (err instanceof Long) {
				err2 = new OpaRpcError(getErrorCode(err));
			} else {
				throw new RuntimeException("unknown error object returned from server: " + OpaUtils.stringify(err));
			}
		}

		try {
			// note that the callback is being called from the response parser
			// thread. This means that all subsequent responses must wait for the callback
			// to finish before being invoked. Therefore the callback must finish
			// quickly (ie, wake up a separate thread if it will not return fast)
			if (err2 != null) {
				cb.onFailure(err2);
			} else {
				cb.onSuccess(result);
			}
		} catch (Exception ex) {
			OpaDef.log("Exception in callback: " + ex.toString());
			//e.printStackTrace();
		}
	}

	private void onResponse(Object o) {
		//if (!(o instanceof List)) {
		//	throw new RuntimeException("Response is not a list");
		//}
		List<?> l = (List<?>) o;
		if (l.size() == 1) {
			handleResponse(l.get(0), null, null);
		} else if (l.size() == 2) {
			handleResponse(l.get(0), l.get(1), null);
		} else if (l.size() == 3) {
			handleResponse(l.get(0), l.get(1), l.get(2));
		} else {
			throw new RuntimeException("Response list is wrong size: " + l.size());
		}
	}

	public void onRecv(byte[] buff, int idx, int len) {
		mBuff.data = buff;
		mBuff.idx = idx;
		mBuff.len = len;
		while (true) {
			Object obj = mParser.parseNext(mBuff);
			if (obj == OpaPartialParser.NOMORE) {
				break;
			}
			onResponse(obj);
			//++mNumRecv;
		}
	}
}
