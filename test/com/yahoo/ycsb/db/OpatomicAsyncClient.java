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
	private boolean mUseDMGET = true;
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
		if (wcb.getError() != null) {
			throw new RuntimeException(wcb.getError().toString());
		}
		return wcb.getResult();
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
		readFields(key, fields, true, result);
		return mError != null || result.isEmpty() ? Status.ERROR : Status.OK;
	}

	private static void resultAddAll(Iterator<?> it, Map<String, ByteIterator> result) {
		while (it.hasNext()) {
			Object k = it.next();
			Object v = it.next();
			result.put((String) k, new ByteArrayByteIterator((byte[]) v));
		}
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

	private static void addAllFromDMGET(List<String> args, Object r, Map<String, ByteIterator> result) {
		List<?> vals = (List<?>) r;
		for (int i = 0; i < vals.size(); ++i) {
			result.put(args.get(i + 1), new ByteArrayByteIterator((byte[]) vals.get(i)));
		}
	}

	private void readFields(final String key, Set<String> fields, boolean wait, final Map<String, ByteIterator> result) {
		if (fields == null || fields.size() == 0) {
			if (wait) {
				Object r = callAndWait("DRANGE", asIt(key));
				resultAddAll(((Iterable<?>) r).iterator(), result);
			} else {
				mClient.call("DRANGE", asIt(key), new CallbackSF<Object,OpaRpcError>() {
					@Override
					public void onFailure(OpaRpcError err) {
						mError = err;
					}
					@Override
					public void onSuccess(Object r) {
						resultAddAll(((Iterable<?>) r).iterator(), result);
					}
				});
			}
		} else {
			if (mUseDMGET) {
				final List<String> args = new ArrayList<String>(1 + fields.size());
				args.add(key);
				args.addAll(fields);
				if (wait) {
					Object r = callAndWait("DMGET", args.iterator());
					addAllFromDMGET(args, r, result);
				} else {
					mClient.call("DMGET", args.iterator(), new CallbackSF<Object,OpaRpcError>() {
						@Override
						public void onFailure(OpaRpcError err) {
							mError = err;
						}
						@Override
						public void onSuccess(Object r) {
							addAllFromDMGET(args, r, result);
						}
					});
				}
			} else {
				ToMap cb = new ToMap(fields.iterator(), result);
				int count = fields.size();
				if (wait && count > 0) {
					--count;
				}
				Iterator<String> it = fields.iterator();
				for (int i = 0; i < count; ++i) {
					mClient.call("DGET", asIt(key, it.next()), cb);
				}
				if (wait && it.hasNext()) {
					String lastField = it.next();
					Object last = callAndWait("DGET", asIt(key, lastField));
					result.put(lastField, new ByteArrayByteIterator((byte[]) last));
				}
			}
		}
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		mError = null;

		WaitCallbackSF<Object,OpaRpcError> wcb = new WaitCallbackSF<Object,OpaRpcError>();
		mClient.call("KEYS", asIt("START", startkey, "LIMIT", recordcount), wcb);
		List<?> keys = (List<?>) waitForResult(wcb);
		int sz = keys.size();
		result.ensureCapacity(sz);
		for (int i = 0; i < sz; ++i) {
			HashMap<String,ByteIterator> record = new HashMap<String,ByteIterator>();
			result.add(record);
			readFields((String) keys.get(i), fields, i == sz - 1, record);
		}

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
