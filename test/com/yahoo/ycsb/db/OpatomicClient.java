/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.yahoo.ycsb.db;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

import com.opatomic.OpaDef;
import com.opatomic.OpaPartialParser;
import com.opatomic.OpaRpcError;
import com.opatomic.OpaSerializer;
import com.opatomic.OpaUtils;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;


// simple client that only performs synchronous operations. errors returned from server throw exception
final class OpaSyncClient {
	// TODO: move this elsewhere? rename? add ctor that accepts Throwable cause?
	public class OpatomicException extends RuntimeException {
		private final OpaRpcError mErr;

		public OpatomicException(OpaRpcError err) {
			mErr = err;
		}

		public OpaRpcError getRpcError() {
			return mErr;
		}
	}

	private final InputStream mIn;
	private final OpaSerializer mSerializer;
	private final OpaPartialParser mParser = new OpaPartialParser();
	private final OpaPartialParser.Buff mPBuff = new OpaPartialParser.Buff();
	private int mPipelineLen = -1;

	public OpaSyncClient(InputStream in, OutputStream out, int buffLen) {
		mIn = in;
		mSerializer = new OpaSerializer(out, buffLen);
		mPBuff.data = new byte[buffLen];
	}

	private void sendRequest(String cmd, Iterator<?> args) throws IOException {
		mSerializer.write(OpaDef.C_ARRAYSTART);
		mSerializer.write(OpaDef.C_NULL);
		mSerializer.writeString(cmd);
		if (args != null) {
			while (args.hasNext()) {
				mSerializer.writeObject(args.next());
			}
		}
		mSerializer.write(OpaDef.C_ARRAYEND);
	}

	// returns 0 if codeObj is invalid
	private static int getErrorCode(Object codeObj) {
		if (codeObj instanceof Long) {
			long code = ((Long) codeObj).longValue();
			if (code > Integer.MAX_VALUE || code < Integer.MIN_VALUE) {
				return 0;
			}
			return (int) code;
		} else if (codeObj instanceof Integer) {
			return ((Integer) codeObj).intValue();
		}
		return 0;
	}

	private static OpaRpcError convertErr(Object err) {
		if (err != null) {
			if (err instanceof List) {
				List<?> lerr = (List<?>) err;
				int lsize = lerr.size();
				if (lsize < 2 || lsize > 3) {
					return new OpaRpcError(OpaDef.ERR_INVRESPONSE, "err array size is wrong", err);
				}
				int code = getErrorCode(lerr.get(0));
				Object msg = lerr.get(1);
				if (code == 0) {
					return new OpaRpcError(OpaDef.ERR_INVRESPONSE, "err code is invalid", err);
				}
				if (!(msg instanceof String)) {
					return new OpaRpcError(OpaDef.ERR_INVRESPONSE, "err msg is not string object", err);
				}
				return new OpaRpcError(code, (String) msg, lsize >= 3 ? lerr.get(2) : null);
			} else if (err instanceof Object[]) {
				Object[] aerr = (Object[]) err;
				List<Object> lerr = new ArrayList<Object>(aerr.length);
				for (int i = 0; i < aerr.length; ++i) {
					lerr.add(aerr[i]);
				}
				return convertErr(lerr);
			} else {
				int code = getErrorCode(err);
				if (code == 0) {
					return new OpaRpcError(OpaDef.ERR_INVRESPONSE, "err code is invalid", err);
				}
				return new OpaRpcError(code);
			}
		}
		return null;
	}

	private OpaRpcError handleErr(boolean throwOnErr, OpaRpcError err) {
		if (throwOnErr) {
			throw new OpatomicException(err);
		}
		return err;
	}

	private Object parseNext(boolean throwOnErr) throws IOException {
		while (true) {
			if (mPBuff.len == 0) {
				int numRead = mIn.read(mPBuff.data);
				if (numRead < 0) {
					throw new EOFException("stream closed");
				}
				mPBuff.idx = 0;
				mPBuff.len = numRead;
			}
			Object o = mParser.parseNext(mPBuff);
			if (o != OpaPartialParser.NOMORE) {
				if (!(o instanceof List)) {
					return handleErr(throwOnErr, new OpaRpcError(OpaDef.ERR_INVRESPONSE, "response not a list", o));
				}

				List<?> r = (List<?>) o;
				int lsz = r.size();

				if (lsz < 2 || lsz > 3) {
					return handleErr(throwOnErr, new OpaRpcError(OpaDef.ERR_INVRESPONSE, "unexpected response list size", o));
				}

				Object id = r.get(0);
				if (id != null) {
					// TODO: handle async id somehow? server sent some sort of message?
					continue;
				}

				Object errObj = lsz > 2 ? r.get(2) : null;
				if (errObj != null) {
					return handleErr(throwOnErr, convertErr(errObj));
				}

				return r.get(1);
			}
		}
	}

	private Object callInternal(boolean throwOnErr, String cmd, Iterator<?> args) {
		try {
			sendRequest(cmd, args);
			if (mPipelineLen < 0) {
				mSerializer.flush();
				return parseNext(throwOnErr);
			} else {
				++mPipelineLen;
				return null;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public Object call(String cmd, Iterator<?> args) {
		return callInternal(true, cmd, args);
	}

	public Object callVA(String cmd, Object... args) {
		return callInternal(true, cmd, Arrays.asList(args).iterator());
	}

	public void startPipeline() {
		if (mPipelineLen >= 0) {
			throw new IllegalStateException("pipeline started already");
		}
		mPipelineLen = 0;
	}

	// note: an exception is thrown if an error is received for any of the responses (all will be lost)
	public Object[] sendPipeline() {
		if (mPipelineLen < 0) {
			throw new IllegalStateException("pipeline not started");
		}
		try {
			// note: could cause problems if a lot of requests and responses are piling up on server...
			//  server may not be able to parse more requests until some responses are received by client?
			mSerializer.flush();
			Object results[] = new Object[mPipelineLen];
			mPipelineLen = -1;
			for (int i = 0; i < results.length; ++i) {
				results[i] = parseNext(true);
			}
			return results;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}


public class OpatomicClient extends DB {
	private Socket mSocket;
	private OpaSyncClient mClient;
	private boolean mInsertStrict;

	@Override
	public void init() throws DBException {
		Properties props = getProperties();
		String host = props.getProperty("opatomic.host", "localhost");
		int port = Integer.parseInt(props.getProperty("opatomic.port", "4567"));
		String pass = props.getProperty("opatomic.password", null);
		mInsertStrict = props.getProperty("opatomic.insertstrict", "false").equalsIgnoreCase("true");
		int buffLen = Integer.parseInt(props.getProperty("opatomic.bufflen", "4096"));
		try {
			mSocket = new Socket(host, port);
			mSocket.setTcpNoDelay(true);
			mClient = new OpaSyncClient(mSocket.getInputStream(), mSocket.getOutputStream(), buffLen);
			if (pass != null) {
				Object r = mClient.callVA("AUTH", pass);
				if (r == Boolean.FALSE || OpaUtils.compare(r, 0) == 0) {
					throw new RuntimeException("auth failed");
				}
			}
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public void cleanup() throws DBException {
		// TODO: call QUIT?
		try {
			if (mSocket != null) {
				mSocket.close();
			}
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	private static void setRangeFields(Iterator<?> it, Map<String, ByteIterator> record) {
		while (it.hasNext()) {
			Object k = it.next();
			record.put((String)k, new ByteArrayByteIterator((byte[]) it.next()));
		}
	}

	private Object[] getFields(List<?> keys, Set<String> fields) {
		mClient.startPipeline();
		for (int i = 0; i < keys.size(); ++i) {
			Iterator<String> it = fields.iterator();
			while (it.hasNext()) {
				mClient.callVA("DGET", keys.get(i), it.next());
			}
		}
		return mClient.sendPipeline();
	}

	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
		if (fields == null) {
			Object r = mClient.callVA("DRANGE", key);
			setRangeFields(((Iterable<?>) r).iterator(), result);
		} else {
			Object results[] = getFields(Arrays.asList(key), fields);
			Iterator<String> it2 = fields.iterator();
			for (int i = 0; i < results.length; ++i) {
				result.put(it2.next(), new ByteArrayByteIterator((byte[]) results[i]));
			}
		}
		return result.isEmpty() ? Status.ERROR : Status.OK;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		// note: this could be performed server-side with a script. would be 1 round trip rather than 2.

		List<?> keys = (List<?>) mClient.callVA("KEYS", "START", startkey, "LIMIT", recordcount);

		if (fields == null) {
			mClient.startPipeline();
			for (int i = 0; i < keys.size(); ++i) {
				mClient.callVA("DRANGE", keys.get(i));
			}
			Object results[] = mClient.sendPipeline();
			for (int i = 0; i < results.length; ++i) {
				HashMap<String,ByteIterator> record = new HashMap<String,ByteIterator>();
				setRangeFields(((Iterable<?>)results[i]).iterator(), record);
				result.add(record);
			}
		} else {
			Object results[] = getFields(keys, fields);
			for (int i = 0; i < keys.size(); ++i) {
				HashMap<String,ByteIterator> record = new HashMap<String,ByteIterator>();
				Iterator<String> it = fields.iterator();
				while (it.hasNext()) {
					record.put(it.next(), new ByteArrayByteIterator((byte[]) results[i]));
				}
				result.add(record);
			}
		}

		return Status.OK;
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
		Object result = mClient.call("DSET", args.iterator());
		return ((Number)result).intValue();
	}

	@Override
	public Status insert(String table, String key, Map<String,ByteIterator> values) {
		// TODO: support batched insertion mode (return Status.BATCHED_OK)
		int response = setall(key, values);
		return (!mInsertStrict || response == values.size()) ? Status.OK : Status.ERROR;
	}

	@Override
	public Status update(String table, String key, Map<String,ByteIterator> values) {
		// note: this does not check whether the key exists. will create key if it does not exist
		setall(key, values);
		return Status.OK;
	}

	@Override
	public Status delete(String table, String key) {
		int result = ((Number)mClient.callVA("DEL", key)).intValue();
		return result == 1 ? Status.OK : Status.ERROR;
	}
}
