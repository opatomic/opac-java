/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.yahoo.ycsb.db;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import com.opatomic.CallbackSF;
import com.opatomic.OpaClient;
import com.opatomic.OpaRpcError;
import com.opatomic.OpaStreamClient;
import com.opatomic.OpaUtils;
import com.opatomic.WaitCallbackSF;


// note: this client is slow because YCSB is designed to run synchronously
public class OpatomicAsyncClient extends DB {
	private Socket mSocket;
	private OpaClient mClient;
	private boolean mInsertStrict;
	private OpaRpcError mError;

	@Override
	public void init() throws DBException {
		Properties props = getProperties();
		String host = props.getProperty("opatomic.host", "localhost");
		int port = Integer.parseInt(props.getProperty("opatomic.port", "4567"));
		String pass = props.getProperty("opatomic.password", null);
		mInsertStrict = props.getProperty("opatomic.insertstrict", "false").equalsIgnoreCase("true");
		String ctype = props.getProperty("opatomic.ctype", OpaStreamClient.class.getName());
		//System.out.println("using " + ctype + " client");

		try {
			if (ctype.equals(OpaStreamClient.class.getName())) {
				mSocket = new Socket(host, port);
				mSocket.setTcpNoDelay(true);
				mClient = new OpaStreamClient(mSocket.getInputStream(), mSocket.getOutputStream());
			//} else if (ctype.equals(OpaNIOClient.class.getName())) {
			//	mClient = OpaNIOClient.connect(new InetSocketAddress(host, port));
			//} else if (ctype.equals(OpaNIOClientST.class.getName())) {
			//	mClient = OpaNIOClientST.connect(new InetSocketAddress(host, port));
			} else {
				throw new RuntimeException("unknown ctype " + ctype);
			}

		} catch (IOException e) {
			throw new DBException(e);
		}

		if (pass != null) {
			Object r = callAndWait("AUTH", asIt(pass));
			if (r == Boolean.FALSE || OpaUtils.compare(r, 0) == 0) {
				throw new RuntimeException("auth failed");
			}
		}
	}

	private static Iterator<Object> asIt(Object... objs) {
		return Arrays.asList(objs).iterator();
	}

	private static Object waitForResult(WaitCallbackSF<Object,OpaRpcError> wcb) {
		try {
			wcb.waitIfNotDone();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		if (wcb.error != null) {
			throw new RuntimeException(wcb.error.toString());
		}
		return wcb.result;
	}

	private Object callAndWait(String cmd, Iterator<?> args) {
		WaitCallbackSF<Object,OpaRpcError> wcb = new WaitCallbackSF<Object,OpaRpcError>();
		mClient.call(cmd, args, wcb);
		return waitForResult(wcb);
	}

	@Override
	public void cleanup() throws DBException {
		// TODO: just close socket rather than calling QUIT and waiting for response?
		WaitCallbackSF<Object,OpaRpcError> wcb = new WaitCallbackSF<Object,OpaRpcError>();
		if (mClient instanceof OpaStreamClient) {
			((OpaStreamClient)mClient).quit("QUIT", null, wcb);
		} else {
			mClient.call("QUIT", null, wcb);
		}
		//mClient.call("QUIT", null, wcb);
		waitForResult(wcb);

		//try {
		//	mSocket.close();
		//} catch (IOException e) {
		//	// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
	}

	@Override
	public Status read(String table, String key, Set<String> fields, final Map<String, ByteIterator> result) {
		if (fields == null) {
			Object r = callAndWait("DRANGE", asIt(key));
			//if (r != null && r instanceof Iterable) {
				Iterator<?> it = ((Iterable<?>) r).iterator();
				while (it.hasNext()) {
					Object k = it.next();
					Object v = it.next();
					result.put((String) k, new ByteArrayByteIterator((byte[]) v));
				}
			//}
		} else {
			final Iterator<String> it1 = fields.iterator();
			CallbackSF<Object,OpaRpcError> cb = new CallbackSF<Object,OpaRpcError>() {
				@Override
				public void onFailure(OpaRpcError e) {
					mError = e;
				}
				@Override
				public void onSuccess(Object r) {
					result.put(it1.next(), new ByteArrayByteIterator((byte[]) r));
				}
			};
			if (fields.size() > 0) {
				Iterator<String> it2 = fields.iterator();
				for (int i = 0; i < fields.size() - 1; ++i) {
					mClient.call("DGET", asIt(key, it2.next()), cb);
				}
				Object last = callAndWait("DGET", asIt(key, it2.next()));
				result.put(it1.next(), new ByteArrayByteIterator((byte[]) last));
			}

			if (mError != null) {
				throw new RuntimeException(mError.toString());
			}
		}

		return result.isEmpty() ? Status.ERROR : Status.OK;
	}

	private final class ToMap implements CallbackSF<Object,OpaRpcError> {
		private final Iterator<String> mFields;
		private final Map<String,ByteIterator> mMap;

		ToMap(Iterator<String> fields, Map<String,ByteIterator> m) {
			mFields = fields;
			mMap = m;
		}
		@Override
		public void onFailure(OpaRpcError err) {
			mError = err;
		}
		@Override
		public void onSuccess(Object r) {
			mMap.put(mFields.next(), new ByteArrayByteIterator((byte[]) r));
		}
	}

	private void readFields(final String key, Set<String> fields, final Map<String, ByteIterator> result) {
		if (fields == null || fields.size() == 0) {
			mClient.call("DRANGE", asIt(key), new CallbackSF<Object,OpaRpcError>() {
				@Override
				public void onFailure(OpaRpcError err) {
					mError = err;
				}
				@Override
				public void onSuccess(Object r) {
					Iterator<?> it = ((List<?>)r).iterator();
					while (it.hasNext()) {
						Object k = it.next();
						Object v = it.next();
						result.put((String)k, new ByteArrayByteIterator((byte[]) v));
					}
				}
			});
		} else {
			ToMap cb = new ToMap(fields.iterator(), result);
			for (Iterator<String> it = fields.iterator(); it.hasNext(); ) {
				mClient.call("DGET", asIt(key, it.next()), cb);
			}
		}
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		mError = null;

		WaitCallbackSF<Object,OpaRpcError> wcb = new WaitCallbackSF<Object,OpaRpcError>();
		mClient.call("KEYS", asIt("START", startkey, "LIMIT", recordcount), wcb);
		List<?> keys = (List<?>) waitForResult(wcb);
		for (int i = 0; i < keys.size(); ++i) {
			HashMap<String,ByteIterator> record = new HashMap<String,ByteIterator>();
			result.add(record);
			readFields((String) keys.get(i), fields, record);
		}

		mClient.call("PING", null, wcb.reset());
		waitForResult(wcb);

		return mError != null ? Status.ERROR : Status.OK;
	}

	private int callIntResult(String cmd, Iterator<Object> args) {
		Object result = callAndWait(cmd, args);
		return ((Number)result).intValue();
	}

	private int setall(String key, Map<String,ByteIterator> values) {
		List<Object> args = new ArrayList<Object>(1 + (2 * values.size()));
		args.add(key);
		Iterator<Map.Entry<String,ByteIterator>> it = values.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String,ByteIterator> e = it.next();
			args.add(e.getKey());
			args.add(e.getValue().toArray());
		}
		return callIntResult("DSET", args.iterator());
	}

	@Override
	public Status insert(String table, String key, Map<String,ByteIterator> values) {
		// TODO: support batched insertion mode (return Status.BATCHED_OK)
		int response = setall(key, values);
		return (!mInsertStrict || response == values.size()) ? Status.OK : Status.ERROR;
	}

	@Override
	public Status update(String table, String key, Map<String,ByteIterator> values) {
		setall(key, values);
		return Status.OK;
	}

	@Override
	public Status delete(String table, String key) {
		int result = callIntResult("DEL", asIt(key));
		return result == 1 ? Status.OK : Status.ERROR;
	}
}
