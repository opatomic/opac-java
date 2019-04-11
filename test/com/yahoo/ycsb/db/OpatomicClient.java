package com.yahoo.ycsb.db;

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
import com.opatomic.OpaSerializer;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;


// simple client that only performs synchronous operations. errors returned from server throw exception
final class OpaSyncClient {
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

	private void sendRequest(String cmd, Iterator<Object> args) throws IOException {
		mSerializer.write(OpaDef.C_ARRAYSTART);
		mSerializer.writeString(cmd);
		if (args != null) {
			mSerializer.writeArray(args);
		}
		mSerializer.write(OpaDef.C_ARRAYEND);
	}

	private void sendRequest(String cmd, Object[] args) throws IOException {
		mSerializer.write(OpaDef.C_ARRAYSTART);
		mSerializer.writeString(cmd);
		if (args != null && args.length > 0) {
			mSerializer.write(OpaDef.C_ARRAYSTART);
			for (int i = 0; i < args.length; ++i) {
				mSerializer.writeObject(args[i]);
			}
			mSerializer.write(OpaDef.C_ARRAYEND);
		}
		mSerializer.write(OpaDef.C_ARRAYEND);
	}

	private List<?> parseNext() throws IOException {
		while (true) {
			if (mPBuff.len == 0) {
				int numRead = mIn.read(mPBuff.data);
				if (numRead < 0) {
					throw new RuntimeException("stream closed");
				}
				mPBuff.idx = 0;
				mPBuff.len = numRead;
			}
			Object o = mParser.parseNext(mPBuff);
			if (o != OpaPartialParser.NOMORE) {
				List<?> r = (List<?>) o;
				int lsz = r.size();
				if (lsz == 0 || lsz > 3) {
					throw new RuntimeException("unexpected response list size");
				}
				Object id = lsz > 2 ? r.get(2) : null;
				if (id != null) {
					// TODO: handle async id somehow? server sent some sort of message?
					continue;
				}
				return r;
			}
		}
	}

	private Object getNextOrThrow() throws IOException {
		List<?> r = parseNext();
		if (r.size() > 1 && r.get(1) != null) {
			throw new RuntimeException("err response received: " + r.get(1).toString());
		}
		return r.get(0);
	}

	public Object call(String cmd, Iterator<Object> args) {
		try {
			sendRequest(cmd, args);
			if (mPipelineLen < 0) {
				mSerializer.flush();
				return getNextOrThrow();
			} else {
				++mPipelineLen;
				return null;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public Object callVA(String cmd, Object... args) {
		try {
			sendRequest(cmd, args);
			if (mPipelineLen < 0) {
				mSerializer.flush();
				return getNextOrThrow();
			} else {
				++mPipelineLen;
				return null;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
				results[i] = getNextOrThrow();
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
				if (r != Boolean.TRUE) {
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

	private void setRangeFields(Iterator<?> it, HashMap<String, ByteIterator> record) {
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
				mClient.callVA("MGET", keys.get(i), it.next());
			}
		}
		return mClient.sendPipeline();
	}

	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		if (fields == null) {
			Object r = mClient.callVA("MRANGE", key);
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
				mClient.callVA("MRANGE", keys.get(i));
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
		Object result = mClient.call("MSET", args.iterator());
		return ((Number)result).intValue();
	}

	@Override
	public Status insert(String table, String key, HashMap<String,ByteIterator> values) {
		// TODO: support batched insertion mode (return Status.BATCHED_OK)
		int response = setall(key, values);
		return (!mInsertStrict || response == values.size()) ? Status.OK : Status.ERROR;
	}

	@Override
	public Status update(String table, String key, HashMap<String,ByteIterator> values) {
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
