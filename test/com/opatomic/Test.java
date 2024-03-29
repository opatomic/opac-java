package com.opatomic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;




class Benchmark {
	private static final Iterator<Object> EMPTYIT = new Iterator<Object>() {
		@Override
		public boolean hasNext() {
			return false;
		}
		@Override
		public Object next() {
			throw new NoSuchElementException();
		}
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	};
	private static final Iterable<Object> EMPTYITERABLE = new Iterable<Object>() {
		@Override
		public Iterator<Object> iterator() {
			return EMPTYIT;
		}
	};

	private static final class State {
		final String cmd;
		final Iterable<Object> args;
		final int numIts;

		private final int mPipeline;
		private final AtomicInteger numStarted = new AtomicInteger();
		private final AtomicInteger numComplete = new AtomicInteger();
		private final Runnable done;

		State(String cmd, Iterable<Object> args, int its, int pipeline, Runnable cb) {
			this.cmd = cmd;
			this.args = args == null ? EMPTYITERABLE : args;
			this.numIts = its;
			this.mPipeline = pipeline;
			this.done = cb;
		}

		int claim() {
			while (true) {
				int started = numStarted.get();
				if (started >= numIts) {
					if (started != numIts) {
						throw new RuntimeException("started too many ops");
					}
					return 0;
				}
				int nextBatch = Math.min(mPipeline, numIts - started);
				if (numStarted.compareAndSet(started, started + nextBatch)) {
					return nextBatch;
				}
			}
		}

		void completed(int count) {
			int completed = numComplete.addAndGet(count);
			if (completed >= numIts) {
				if (completed != numIts) {
					throw new RuntimeException("completed too many ops");
				}
				done.run();
				return;
			}
		}
	}

	private static final class ClientRunner {
		private final OpaClient mClient;
		private State mState;
		private int mCurrBatch;

		ClientRunner(OpaClient c) {
			mClient = c;
		}

		private final CallbackSF<Object,OpaRpcError> mBatchCB = new CallbackSF<Object,OpaRpcError>() {
			@Override
			public void onSuccess(Object result) {
				mState.completed(mCurrBatch);
				claimAndRun();
			}
			@Override
			public void onFailure(OpaRpcError error) {
				System.out.println("ERROR: " + error.toString());
			}
		};

		private void claimAndRun() {
			int its = mState.claim();
			if (its > 0) {
				mCurrBatch = its;
				for (int i = 1; i < its; ++i) {
					mClient.call(mState.cmd, mState.args.iterator(), Test.ECHOERRCB);
				}
				mClient.call(mState.cmd, mState.args.iterator(), mBatchCB);
			}
		}

		public void runCommand(State s) {
			mState = s;
			claimAndRun();
		}
	}


	private ClientRunner mClients[];

	/*
	void connect(String host, int port, int numClients) throws IOException {
		mClients = new ClientRunner[numClients];
		for (int i = 0; i < numClients; ++i) {
			OpaNioClient c = OpaNioClient.connect(new InetSocketAddress(host, port));

			//Socket s = new Socket("127.0.0.1", 4567);
			//s.setTcpNoDelay(true);
			//OpaStreamClient c = new OpaStreamClient(s.getInputStream(), s.getOutputStream());

			mClients[i] = new ClientRunner(c);
		}
	}
	*/

	void runCommand(String cmd, Iterable<Object> args, int its, int pipeline) {
		Runnable donecb = new Runnable() {
			@Override
			public synchronized void run() {
				this.notify();
			}
		};
		State s = new State(cmd, args, its, pipeline, donecb);
		synchronized (donecb) {
			for (int i = 0; i < mClients.length; ++i) {
				mClients[i].runCommand(s);
			}
			try {
				donecb.wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}
}




public class Test {
	public static final class EchoCB implements CallbackSF<Object, OpaRpcError> {
		@Override
		public void onSuccess(Object result) {
			System.out.println(OpaUtils.stringify(result));
		}
		@Override
		public void onFailure(OpaRpcError error) {
			System.out.println("ERROR: " + error.toString());
		}
	}

	public static final class EchoErrCB implements CallbackSF<Object, OpaRpcError> {
		@Override
		public void onSuccess(Object result) {
			//System.out.println(stringify(result));
		}
		@Override
		public void onFailure(OpaRpcError error) {
			System.out.println("ERROR: " + error.toString());
		}
	}

	private static final CallbackSF<Object, OpaRpcError> ECHOCB = new EchoCB();
	static final CallbackSF<Object, OpaRpcError> ECHOERRCB = new EchoErrCB();






	private static Object callSync(OpaClient c, String cmd, Iterator<Object> args) {
		WaitCallbackSF<Object,OpaRpcError> wcb = new WaitCallbackSF<Object,OpaRpcError>();
		c.call(cmd, args, wcb);
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

	private static byte[] serializeToBuff(Object o) {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			OpaSerializer s = new OpaSerializer(out, 1024);
			s.writeObject(o);
			s.flush();
			s.close();
			return out.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static Object parseBuff(byte[] bytes) {
		OpaPartialParser pp = new OpaPartialParser();
		OpaPartialParser.Buff b = new OpaPartialParser.Buff();

		//b.data = bytes;
		//b.idx = 0;
		//b.len = b.data.length;
		//Object check = pp.parseNext(b);
		//if (check == null || pp.parseNext(b) != null || b.len != 0) {
		//	throw new RuntimeException();
		//}
		//return check;

		Object check = null;
		b.data = bytes;
		for (int i = 0; i < bytes.length; ++i) {
			b.idx = i;
			b.len = 1;
			check = pp.parseNext(b);
			if (check != OpaPartialParser.NOMORE && i != bytes.length - 1) {
				throw new RuntimeException();
			}
		}
		if (check == null) {
			throw new RuntimeException();
		}
		return check;
	}

	private static Iterable<Object> asList(Object... objs) {
		return Arrays.asList(objs);
	}

	private static Iterator<Object> asIt(Object... objs) {
		return asList(objs).iterator();
	}



	private static void checkMinByte(Object o1, Object o2, byte[] b, int ch) {
		if (OpaUtils.compare(o1, o2) == 0) {
			if (b.length != 1 || b[0] != ch) {
				throw new RuntimeException();
			}
		}
	}

	// check whether byte array is minimal representation of object
	private static void checkMin(Object o, byte[] b) {
		checkMinByte(o, OpaDef.ZeroObj,       b, OpaDef.C_ZERO);
		checkMinByte(o, OpaDef.EmptyBinObj,   b, OpaDef.C_EMPTYBIN);
		checkMinByte(o, OpaDef.EmptyStrObj,   b, OpaDef.C_EMPTYSTR);
		checkMinByte(o, OpaDef.EmptyArrayObj, b, OpaDef.C_EMPTYARRAY);
	}

	private static void testVal2(Object o) {
		byte[] b = serializeToBuff(o);
		checkMin(o, b);

		ArrayList<Object> l = new ArrayList<Object>();
		l.add(o);
		o = l;
		Object check = parseBuff(serializeToBuff(o));
		if (check == null || OpaUtils.compare(o, check) != 0) {
			throw new RuntimeException();
		}
	}

	private static void getAllNums3(BigDecimal bd, Collection<Object> vals) {
		// get all representations of the number
		vals.add(bd.byteValue());
		vals.add(bd.shortValue());
		vals.add(bd.intValue());
		vals.add(bd.longValue());
		float fl = bd.floatValue();
		if (!Float.isInfinite(fl) && !Float.isNaN(fl)) {
			vals.add(fl);
		}
		double db = bd.doubleValue();
		if (!Double.isInfinite(db) && !Double.isNaN(db)) {
			vals.add(db);
		}
		vals.add(bd.toBigInteger());
		vals.add(bd.unscaledValue());
	}

	private static void getAllNums2(BigDecimal bd, Collection<Object> vals) {
		getAllNums3(bd, vals);
		getAllNums3(bd.negate(), vals);
	}

	private static void getAllNums(BigDecimal bd, Collection<Object> vals) {
		getAllNums2(bd, vals);
		getAllNums2(bd.add(BigDecimal.ONE), vals);
		getAllNums2(bd.subtract(BigDecimal.ONE), vals);
	}

	private static void testVal(Object o) {
		testVal2(o);
		BigDecimal bval = OpaUtils.getBig(o);
		if (bval != null) {
			List<Object> l = new ArrayList<Object>();
			getAllNums(bval, l);
			for (int i = 0; i < l.size(); ++i) {
				testVal2(l.get(i));
			}
		}
	}

	private static Object[] TESTVALS = {
		OpaDef.UndefinedObj, Boolean.FALSE, Boolean.TRUE, OpaDef.ZeroObj,
		OpaDef.EmptyBinObj, OpaDef.EmptyStrObj, OpaDef.EmptyArrayObj,
		null, false, true, 0,
		"", new byte[0], new Object[0], new ArrayList<Object>(),
		Byte.MIN_VALUE, Byte.MAX_VALUE, Short.MIN_VALUE, Short.MAX_VALUE,
		Integer.MIN_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE,
		Float.MIN_VALUE, Float.MIN_NORMAL, Float.MAX_VALUE,
		Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE,
		Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY,
		Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
		new BigDecimal("1.23"), new BigDecimal("1.23e-4"),
		new BigDecimal("2398490238498230948029384092390479812390170293809128309183098129038190237104789"),
		new BigDecimal("-9832749023794872893479287498237894739827498237984783947"),
		new BigDecimal("-9023804982093480197043971093701928309.982910810298309130981290380192830983"),
		new Object[] {0, 1, 2, "", false, null, -55, new Object[] {Long.MIN_VALUE, 0, "hello", new Object[] {"string", 87}}, new byte[0], new byte[] {0,1,2,3}},
		new Object[] {new Object[0]},
	};

	private static void testSerialize() {

		for (int i = 0; i < TESTVALS.length; ++i) {
			testVal(TESTVALS[i]);
		}

		//testVal(new BigDecimal("9327498273984724e" + Integer.toString(Integer.MIN_VALUE + 1)));
		//testVal(new BigDecimal("9327498273984724e" + Integer.toString(Integer.MAX_VALUE)));
		//testVal(new BigDecimal("-9327498273984724e" + Integer.toString(Integer.MIN_VALUE + 1)));
		//testVal(new BigDecimal("-9327498273984724e" + Integer.toString(Integer.MAX_VALUE)));

	}

	private static long bench2(OpaClient c, int its, String command, Iterable<Object> args, boolean async) throws InterruptedException {
		WaitCallbackSF<Object,OpaRpcError> wcb = new WaitCallbackSF<Object,OpaRpcError>();
		long time = System.currentTimeMillis();
		for (--its; its > 0; --its) {
			if (async) {
				c.callA(command, args == null ? null : args.iterator(), ECHOERRCB);
			} else {
				c.call(command, args == null ? null : args.iterator(), ECHOERRCB);
			}
		}
		c.call(command, args == null ? null : args.iterator(), wcb);
		wcb.waitIfNotDone();
		return System.currentTimeMillis() - time;
	}

	private static void bench(OpaClient c, int its, String command, Iterable<Object> args, boolean async) throws InterruptedException {
		for (int i = 0; i < 2; ++i) {
			System.out.println(command + " time: " + bench2(c, its, command, args, async));
		}
	}

	private static void bench(OpaClient c, int its, String command, Iterable<Object> args) throws InterruptedException {
		bench(c, its, command, args, false);
	}

	private static void bench(OpaClient c, int its, String command) throws InterruptedException {
		bench(c, its, command, null);
	}

	private static void check(OpaClient c, final Object expect, final String command, final Object... args) {
		c.call(command, asIt(args), new CallbackSF<Object,OpaRpcError>() {
			@Override
			public void onSuccess(Object result) {
				if (OpaUtils.compare(expect, result) != 0) {
					System.out.println("unexpected result for command: " + command + " " + asList(args).toString() + "; " + OpaUtils.stringify(result));
				}
			}
			@Override
			public void onFailure(OpaRpcError error) {
				System.err.println("failure for command: " + command + "; " + error.toString());
			}
		});
	}

	private static void createBigBlob(OpaClient c, int chunkLen, int numChunks) {
		Object blen = callSync(c, "BLEN", asIt("bigblob"));

		if (OpaUtils.compare(blen, chunkLen * numChunks) != 0) {
			//System.out.println("creating bigblob...");
			c.call("DEL", asIt("bigblob"), ECHOERRCB);
			byte[] chunk = new byte[chunkLen];
			for (int i = 0; i < chunkLen; ++i) {
				chunk[i] = (byte) i;
			}
			for (int i = 0; i < numChunks; ++i) {
				c.call("BAPPEND", asIt("bigblob", chunk), ECHOERRCB);
			}
			System.out.println("created bigblob");
		} else {
			System.out.println("bigblob already exists");
		}
	}

	private static void populateMap(OpaClient c, String key, int its, int chunkLen) {
		long time = System.currentTimeMillis();

		for (int i = 0; i < its; ++i) {
			List<Object> args = new ArrayList<Object>(1 + chunkLen);
			args.add(key);
			for (int j = 0; j < chunkLen; ++j) {
				args.add((i * chunkLen) + j);
				args.add((i * chunkLen) + j);
				//String s = Integer.toString((i * chunkLen) + j);
				//args.add(s);
				//args.add(s);
			}
			c.call("DSET", args.iterator(), null);
		}
		callSync(c, "PING", null);
		System.out.println("populateMap " + (its*chunkLen) + ": " + (System.currentTimeMillis() - time));
	}

	private static void loadServerSend(OpaClient c, int its) {
		createBigBlob(c, 1024, 10000);

		long time = System.currentTimeMillis();

		for (int i = 0; i < its; ++i) {
			c.call("BGETRANGE", asIt("bigblob", 0, -1), ECHOERRCB);
		}

		callSync(c, "PING", null);

		System.out.println("bulk blob time: " + (System.currentTimeMillis() - time));
	}

	/**
	 * call the specified command+args; wait until response is received before sending next request
	 * @param c     client
	 * @param op    command to run
	 * @param args  command arguments
	 * @param its   number of times to run command
	 */
	private static void testSyncCalls(final OpaClient c, final String op, final Iterable<Object> args, final int its) {
		long time = System.currentTimeMillis();

		CallbackSF<Object,OpaRpcError> cb = new CallbackSF<Object,OpaRpcError>() {
			private int mIts = its;
			@Override
			public void onSuccess(Object result) {
				--mIts;
				if (mIts > 0) {
					c.call(op, args == null ? null : args.iterator(), this);
				} else {
					synchronized (this) {
						this.notify();
					}
				}
			}
			@Override
			public void onFailure(OpaRpcError error) {
				System.out.println("error: " + error);
			}
		};

		synchronized (cb) {
			c.call(op, args == null ? null : args.iterator(), cb);
			try {
				cb.wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.out.println("sync call " + op + " time: " + (System.currentTimeMillis() - time));
	}

	private static void testCloseFromSerializerException(String host, int port) {
		try {
			WaitCallbackSF<Object,OpaRpcError> wcb = new WaitCallbackSF<Object,OpaRpcError>();
			Socket s = new Socket(host, port);
			s.setTcpNoDelay(true);
			OpaStreamClient c = new OpaStreamClient(s.getInputStream(), s.getOutputStream());
			c.call("ECHO", asIt(new OpaSerializer.OpaSerializable() {
				@Override
				public void writeOpaSO(OpaSerializer out) {
					throw new RuntimeException("test case!");
				}
			}), wcb);
			wcb.waitIfNotDone();
			if (wcb.getError() == null) {
				throw new RuntimeException("expected error in cb");
			}
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("exception thrown when none should occur");
		}
	}

	// TODO: latency tester
	// TODO: tester with many concurrent clients loading the db
	// TODO: tests to validate the db's ops are implemented correctly

	private static void bench(OpaClient c) throws InterruptedException {
		bench(c, 1000000, "PING");
		bench(c, 1000000, "PING");
		bench(c, 1000000, "PING", null, true);
		bench(c, 1000000, "PING", null, true);
		//bench(c, 1000000, "ECHO", asList(0));
		//bench(c, 1000000, "ECHO", asList("abcdefghijklmnopqrstuvwxyz0123456789"));
		//bench(c, 1000000, "ECHO", asList(Long.MAX_VALUE));
		//bench(c, 1000000, "ECHO", asList(0 - Long.MAX_VALUE));
		//bench(c, 1000000, "ECHO", asList(BigInteger.valueOf(0 - Long.MAX_VALUE)));
		//bench(c, 1000000, "ECHO", asList(BigDecimal.valueOf(0 - Long.MAX_VALUE)));
		//bench(c, 400000, "ECHO", asList("abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789"));
		bench(c, 1000000, "ECHO", asList(new BigInteger("92038492839048209384902834902839048209384902834902830948230948092384902839408239048920384902834902834")));
		//bench(c, 1000000, "ECHO", asList(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
		//bench(c, 1000000, "INCR", asList("i1"));
		//bench(c, 1000000, "RPUSH", asList("L1", 0));
	}

	private static void testSubMany(OpaClient c, int count) {
		c.registerCB("_pubsub", ECHOERRCB);
		for (int i = 0; i < count; ++i) {
			c.call("SUBSCRIBE", asIt("ch" + i), ECHOERRCB);
		}
		for (int i = 0; i < count; ++i) {
			c.call("UNSUBSCRIBE", asIt("ch" + i), ECHOERRCB);
		}
		callSync(c, "PING", null);
		c.call("ECHO", asIt("CHANNELS (on next line):"), ECHOCB);
		c.call("PUBSUB", asIt("CHANNELS"), ECHOCB);
		c.registerCB("_pubsub", null);
	}

	private static SSLContext trustAllContext() throws KeyManagementException, NoSuchAlgorithmException {
		SSLContext c = SSLContext.getInstance("TLS");

		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[] {
				new X509TrustManager() {
					@Override
					public java.security.cert.X509Certificate[] getAcceptedIssuers() {
						return new java.security.cert.X509Certificate[] {};
					}
					@Override
					public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
					@Override
					public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
				}
		};

		c.init(null, trustAllCerts, null);

		return c;
	}

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		try {

			String host = "localhost";
			int port = 4567;

			testSerialize();

			testCloseFromSerializerException(host, port);

			Socket s = new Socket(host, port);
			if (!s.getInetAddress().isLoopbackAddress()) {
				// TODO: handle --cacert ca.crt, --sni opad, etc on command line and validate server identity?
				s = trustAllContext().getSocketFactory().createSocket(s, host, port, true);
			}
			s.setTcpNoDelay(true);

			OpaStreamClient c = new OpaStreamClient(s.getInputStream(), s.getOutputStream());

			//OpaClient c = OpaNio2Client.connect(new InetSocketAddress(host, port), 3, TimeUnit.SECONDS, cfg);
			//OpaClient c = OpaNioClient.connect(new InetSocketAddress(host, port), 3000, cfg);


			WaitCallbackSF<Object,OpaRpcError> wcb = new WaitCallbackSF<Object,OpaRpcError>();

			c.call("PING", null, ECHOCB);
			//c.call("ECHO", asIt(asIt("hello", 0, 1, -2)), ECHOCB);
			//c.call("INFO", null, ECHOCB);

			c.callA("SLEEP", asIt(500), ECHOCB);
			c.callA("SLEEP", asIt(600), ECHOCB);
			c.callA("SLEEP", asIt(700), ECHOCB);
			c.call("SLEEP", asIt(2000), ECHOCB);

			c.callA("ECHO", asIt("echo1 while sleeping"), ECHOCB);
			c.callA("ECHO", asIt("echo2 while sleeping"), ECHOCB);
			c.callA("INVALIDCMD", asIt("err while sleeping"), ECHOCB);
			c.callA("ECHO", asIt("echo3 while sleeping"), ECHOCB);

			c.call("PING", null, ECHOCB);

			c.call("PING", null, null);

			c.call("ECHO", asIt(asIt("hello", "arg2", "goodbye")), ECHOCB);
			c.call("ECHO", asIt(OpaDef.UndefinedObj), ECHOCB);
			c.call("ECHO", asIt(), ECHOCB);
			//c.call("ECHO", asIt(OpaDef.NullObj), ECHOCB);
			c.call("ECHO", asIt(asIt(OpaDef.UndefinedObj, null, false, true, OpaDef.EmptyBinObj,"",new Object[0])), ECHOCB);
			c.call("ECHO", asIt(asIt(new BigDecimal("-123e-4"))), ECHOCB);

			c.call("INVALIDCMD", asIt(asIt("arg1", "arg2", "arg3")), ECHOCB);

			check(c, "PONG", "PING");

			check(c, 1, "INCR", "i1");
			check(c, 0, "INCR", "i1", -1);
			check(c, -2, "INCR", "i1", -2);
			//Thread.sleep(2);

			c.call("INCR", asIt("i2", new BigDecimal("123e-41")), ECHOCB);
			c.call("INCR", asIt("i2", new BigDecimal("123e-41")), ECHOCB);

			c.registerCB("_pubsub", ECHOCB);
			c.call("SUBSCRIBE", asIt("ch1"), ECHOCB);
			c.call("PUBSUB", asIt("NUMSUB", "ch1"), ECHOCB);
			c.call("PUBSUB", asIt("CHANNELS"), ECHOCB);
			c.call("PUBLISH", asIt("ch1", "msg1"), ECHOCB);
			c.call("PUBLISH", asIt("ch1", "msg2"), ECHOCB);
			c.call("UNSUBSCRIBE", asIt("ch1"), ECHOCB);
			callSync(c, "PING", null);
			c.registerCB("_pubsub", null);

			testSubMany(c, 100000);

			c.call("PING", null, ECHOCB);
			/*
			List<Object> l = new ArrayList<Object>();
			l.add("abcdefghijklmnopqrstuvwxyz0123456789");

			long time = System.currentTimeMillis();
			for (int i = 0; i < 1000000; ++i) {
				//c.call("PING", null, null);
				c.call("ECHO", l.iterator(), null);
			}
			c.call("PING", null, wcb);
			wcb.waitIfNotDone();
			System.out.println("time: " + Long.toString(System.currentTimeMillis() - time));
			*/

			boolean runBench = false;
			if (runBench) {
				bench(c);
			}


			//populateMap(c, "testBigMap", 50000, 100);
			//loadServerSend(c, 100);
			//testSyncCalls(c, "PING", null, 100000);
			//testSyncCalls(c, "INCR", asList("i1"), 100000);
			//testSyncCalls(c, "INCR", asList("i1", 2), 100000);


			/*
			int ops = 100000;
			int pipeline = 50;
			Benchmark b = new Benchmark();
			b.connect(host, port, 50);
			//b.runCommand("ECHO", asList(123), 10000);
			//b.runCommand("ECHO", asList(124), 10000);
			b.runCommand("PING", null, ops, pipeline);
			b.runCommand("PING", null, ops, pipeline);
			long time = System.currentTimeMillis();
			b.runCommand("ECHO", asList(125), ops, pipeline);
			b.runCommand("PING", null, ops, pipeline);
			b.runCommand("INCR", asList("i1"), ops, pipeline);
			b.runCommand("LLEN", asList("l1"), ops, pipeline);
			b.runCommand("RPUSH", asList("l1", 1234567890), ops, pipeline);
			b.runCommand("RPUSH", asList("l1", "abc"), ops, pipeline);
			b.runCommand("RPUSH", asList("l1", "abc".getBytes()), ops, pipeline);
			b.runCommand("LRANGE", asList("l1", 0, 100), ops, pipeline);
			b.runCommand("LRANGE", asList("l1", 0, 300), ops, pipeline);
			b.runCommand("LRANGE", asList("l1", 0, 450), ops, pipeline);
			b.runCommand("LRANGE", asList("l1", 0, 600), ops, pipeline);
			time = System.currentTimeMillis() - time;
			System.out.println("bench time: " + time);
			System.out.println("IOPS: " + ((ops * 1000) / time));
			*/


			//Thread.sleep(100);

			c.call("PING", null, ECHOCB);
			c.call("PING", null, wcb.reset());
			wcb.waitIfNotDone();

			//Thread.sleep(123000);

			//c.quit("QUIT", null, wcb.reset());
			c.call("QUIT", null, wcb.reset());
			wcb.waitIfNotDone();

			//s.close();


			//System.out.println(OpaUtils.stringify(HEXCHARS));
			System.out.println(OpaUtils.stringify("~"));
			System.out.println(OpaUtils.stringify("~hello"));
			System.out.println(OpaUtils.stringify(TESTVALS));

			StringBuilder sb = new StringBuilder();
			for (char i = 0; i < 0x80; ++i) {
				sb.append(i);
			}
			System.out.println(OpaUtils.stringify(sb.toString()));


		} catch (Exception e) {
			System.out.println("exception caught in main()");
			e.printStackTrace();
		}
	}
}
