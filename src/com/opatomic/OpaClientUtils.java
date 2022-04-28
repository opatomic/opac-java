package com.opatomic;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

class OpaClientUtils {
	static final OpaRpcError CLOSED_ERROR = new OpaRpcError(OpaDef.ERR_CLOSED);

	static class ResponseCallbackFailure {
		public final CallbackSF<Object,OpaRpcError> callback;
		public final Object responseAsyncId;
		public final Object responseResult;
		public final OpaRpcError responseError;
		public ResponseCallbackFailure(CallbackSF<Object,OpaRpcError> cb, Object id, Object result, OpaRpcError err) {
			callback = cb;
			responseAsyncId = id;
			responseResult = result;
			responseError = err;
		}
	}

	static void invokeCallback(OpaClientConfig cfg, CallbackSF<Object,OpaRpcError> cb, Object result, OpaRpcError err) {
		try {
			if (cb != null) {
				if (err != null) {
					cb.onFailure(err);
				} else {
					cb.onSuccess(result);
				}
			}
		} catch (Exception ex) {
			if (cfg.uncaughtExceptionHandler != null) {
				cfg.uncaughtExceptionHandler.handle(ex, new ResponseCallbackFailure(cb, null, null, err));
			}
		}
	}

	static void writeRequest(OpaSerializer s, CharSequence cmd, Iterator<?> args, Object id) throws IOException {
		s.write(OpaDef.C_ARRAYSTART);
		s.writeObject(id);
		if (cmd == null) {
			s.writeObject(cmd);
		} else {
			s.writeString(cmd);
		}
		if (args != null) {
			while (args.hasNext()) {
				s.writeObject(args.next());
			}
		}
		s.write(OpaDef.C_ARRAYEND);
	}

	static void respondWithClosedErr(OpaClientConfig cfg, Queue<CallbackSF<Object,OpaRpcError>> mainCBs, Map<Object,CallbackSF<Object,OpaRpcError>> asyncCBs) {
		// notify callbacks that conn is closed
		while (true) {
			CallbackSF<Object,OpaRpcError> cb = mainCBs.poll();
			if (cb == null) {
				break;
			}
			invokeCallback(cfg, cb, null, CLOSED_ERROR);
		}

		if (asyncCBs.size() > 0) {
			Iterator<Map.Entry<Object,CallbackSF<Object,OpaRpcError>>> it = asyncCBs.entrySet().iterator();
			while (it.hasNext()) {
				invokeCallback(cfg, it.next().getValue(), null, CLOSED_ERROR);
			}
			asyncCBs.clear();
		}
	}
}
