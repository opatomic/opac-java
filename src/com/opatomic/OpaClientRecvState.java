package com.opatomic;

import java.util.List;
import java.util.Map;
import java.util.Queue;

class OpaClientRecvState {
	private final Queue<CallbackSF<Object,Object>> mMainCallbacks;
	private final Map<Long,CallbackSF<Object,Object>> mAsyncCallbacks;
	
	private final OpaPartialParser.Buff mBuff = new OpaPartialParser.Buff();
	private final OpaPartialParser mParser = new OpaPartialParser();
	
	//private long mNumRecv;

	public OpaClientRecvState(Queue<CallbackSF<Object,Object>> maincbs, Map<Long,CallbackSF<Object,Object>> asynccbs) {
		mMainCallbacks = maincbs;
		mAsyncCallbacks = asynccbs;
	}
	
	private void handleResponse(Object result, Object err, Object id) {
		CallbackSF<Object,Object> cb;
		if (id != null) {
			//if (!(id instanceof Long)) {
			//	throw new RuntimeException("id is not a Long");
			//}
			cb = ((Long)id).longValue() < 0 ? mAsyncCallbacks.get(id) : mAsyncCallbacks.remove(id);
			if (cb == null) {
				throw new RuntimeException("Unknown callback id " + id.toString());
			}
		} else {
			cb = mMainCallbacks.remove();
		}
		
		try {
			// note that the callback is being called from the response parser
			// thread. This means that all subsequent responses must wait for the callback
			// to finish before being invoked. Therefore the callbacks must finish
			// quickly (ie, wake up a separate thread if it will not return fast)
			if (err != null && err != OpaDef.UndefinedObj) {
				cb.onFailure(err);
			} else {
				cb.onSuccess(result);
			}
		} catch (Exception e) {
			OpaDef.log("Exception in callback: " + e.toString());
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
