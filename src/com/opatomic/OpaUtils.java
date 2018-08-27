package com.opatomic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class OpaUtils {
	private static final Charset UTF8CS = Charset.forName("UTF-8");
	private static final byte[] B64PREFIX = "\"~b64".getBytes();
	private static final byte[] BINPREFIX = "\"~bin".getBytes();
	private static final byte[] NULLCHARS = "null".getBytes();
	private static final byte[] HEXCHARS = "0123456789abcdef".getBytes();

	private static final byte[] ENC_TABLE = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".getBytes();


	private static byte[] base64Encode(byte[] buff, int pos, int len, boolean appendEquals) {
		int end = pos + len - 2;
		int numBytes = ((len / 3) << 2);
		int rem = len % 3;
		if (rem > 0) {
			numBytes += appendEquals ? 4 : rem + 1;
		}
		byte[] enc = new byte[numBytes];
		int destPos = 0;
		while (pos < end) {
			enc[destPos++] = ENC_TABLE[((buff[pos] & 0xFC) >> 2)];
			enc[destPos++] = ENC_TABLE[((buff[pos++] & 0x03) << 4) | ((buff[pos] & 0xF0) >> 4)];
			enc[destPos++] = ENC_TABLE[((buff[pos++] & 0x0F) << 2) | ((buff[pos] & 0xC0) >> 6)];
			enc[destPos++] = ENC_TABLE[(buff[pos++] & 0x3F)];
		}
		if (rem == 1) {
			enc[destPos++] = ENC_TABLE[((buff[pos] & 0xFC) >> 2)];
			enc[destPos++] = ENC_TABLE[((buff[pos] & 0x03) << 4)];
			if (appendEquals) {
				enc[destPos++] = '=';
				enc[destPos] = '=';
			}
		} else if (rem == 2) {
			enc[destPos++] = ENC_TABLE[((buff[pos] & 0xFC) >> 2)];
			enc[destPos++] = ENC_TABLE[((buff[pos++] & 0x03) << 4) | ((buff[pos] & 0xF0) >> 4)];
			enc[destPos++] = ENC_TABLE[((buff[pos] & 0x0F) << 2)];
			if (appendEquals) {
				enc[destPos] = '=';
			}
		}
		return enc;
	}

	private static final class ObjIterator<T> implements Iterator<T> {
		private T[] mVals;
		private int mIdx;
		private int mStop;
		ObjIterator(T[] a) {
			this(a, 0, a.length);
		}
		ObjIterator(T[] a, int idx, int len) {
			mVals = a;
			mIdx = idx;
			mStop = idx + len;
		}
		@Override
		public boolean hasNext() {
			return mIdx < mStop ? true : false;
		}
		@Override
		public T next() {
			if (mIdx >= mStop) {
				throw new NoSuchElementException();
			}
			return mVals[mIdx++];
		}
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private static Iterator<?> getIt(Object o) {
		if (o instanceof Iterator) {
			return (Iterator<?>) o;
		} else if (o instanceof Iterable) {
			return ((Iterable<?>)o).iterator();
		} else if (o instanceof Object[]) {
			return new ObjIterator<Object>((Object[]) o);
		}
		throw new IllegalArgumentException("Unsupported type: " + o.getClass().toString());
	}

	private static void writeStringBytes(byte[] bytes, OutputStream out) throws IOException {
		for (int i = 0; i < bytes.length; ++i) {
			int b = bytes[i];
			switch (b) {
				case '"':  out.write('\\'); out.write('"');  break;
				case '\\': out.write('\\'); out.write('\\'); break;
				case '\t': out.write('\\'); out.write('t');  break;
				case '\r': out.write('\\'); out.write('r');  break;
				case '\n': out.write('\\'); out.write('n');  break;
				default:
					if (b < 0x20) {
						out.write('\\');
						out.write('u');
						out.write('0');
						out.write('0');
						out.write(HEXCHARS, (b & 0xF0) >> 4, 1);
						out.write(HEXCHARS, (b & 0x0F), 1);
					} else {
						out.write(b);
					}
			}
		}
	}

	private static void stringify(String s, OutputStream out) throws IOException {
		byte[] bytes = s.getBytes(UTF8CS);
		out.write('"');
		if (bytes.length > 0 && (bytes[0] == '~' || bytes[0] == '^' || bytes[0] == '`')) {
			// if first char is ~ or ^ or ` then it must be escaped
			out.write('~');
		}
		writeStringBytes(bytes, out);
		out.write('"');
	}

	private static void stringify(byte[] buff, OutputStream out) throws IOException {
		for (int i = 0; i < buff.length; ++i) {
			int ch = buff[i];
			if (ch < 0x20 && ch != '\r' && ch != '\n' && ch != '\t') {
				out.write(B64PREFIX);
				out.write(base64Encode(buff, 0, buff.length, true));
				out.write('"');
				return;
			}
		}
		// if chars are all ascii then use different prefix and do not encode as base-64
		out.write(BINPREFIX);
		writeStringBytes(buff, out);
		out.write('"');
	}

	private static void writeIndent(byte[] space, int depth, OutputStream out) throws IOException {
		if (space != null && space.length > 0) {
			out.write('\n');
			for (; depth > 0; --depth) {
				out.write(space);
			}
		}
	}

	private static void stringify(Object o, byte[] space, int depth, OutputStream out) throws IOException {
		if (o == null) {
			out.write(NULLCHARS);
		} else if (o instanceof String) {
			stringify((String)o, out);
		} else if (o instanceof byte[]) {
			stringify((byte[]) o, out);
		} else if (o instanceof Iterable || o instanceof Object[]) {
			Iterator<?> it = getIt(o);
			if (!it.hasNext()) {
				out.write('[');
				out.write(']');
				return;
			}
			/*
			if (space == null || space.length == 0) {
				out.write('[');
				while (true) {
					stringify(it.next(), space, depth + 1, out);
					if (!it.hasNext()) {
						break;
					}
					out.write(',');
				}
				out.write(']');
				return;
			}
			*/
			out.write('[');
			writeIndent(space, depth + 1, out);
			while (true) {
				stringify(it.next(), space, depth + 1, out);
				if (!it.hasNext()) {
					break;
				}
				out.write(',');
				writeIndent(space, depth + 1, out);
			}
			writeIndent(space, depth, out);
			out.write(']');
		} else {
			// TODO: throw exception for Double/Float objects when they are NaN or +/-Infinity (not finite)
			out.write(o.toString().getBytes(UTF8CS));
		}
	}

	public static String stringify(Object o, String space) {
		byte[] spaceBytes = space == null || space.length() == 0 ? null : space.getBytes(UTF8CS);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			stringify(o, spaceBytes, 0, out);
			return out.toString("UTF-8");
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static String stringify(Object o) {
		return stringify(o, null);
	}


	private static final int T_UNDEFINED = 0x01;
	private static final int T_NULL      = 0x02;
	private static final int T_FALSE     = 0x03;
	private static final int T_TRUE      = 0x04;
	private static final int T_NUMBER    = 0x05;
	private static final int T_BIN       = 0x06;
	private static final int T_STRING    = 0x07;
	private static final int T_ARRAY     = 0x08;

	private static int getType(Object o) {
		// TODO: map class to type rather than if/else + instanceof?
		if (o == null) {
			return T_NULL;
		} else if (o == OpaDef.UndefinedObj) {
			return T_UNDEFINED;
		} else if (o instanceof String) {
			return T_STRING;
		} else if (o instanceof Integer || o instanceof Long) {
			return T_NUMBER;
		} else if (o instanceof Iterable) {
			return T_ARRAY;
		} else if (o instanceof Object[]) {
			return T_ARRAY;
		} else if (o instanceof byte[]) {
			return T_BIN;
		} else if (o instanceof Boolean) {
			return ((Boolean)o).booleanValue() ? T_TRUE : T_FALSE;
		} else if (o instanceof Double) {
			//if (!Double.isFinite(((Double)o).doubleValue())) {
			Double v = (Double) o;
			if (v.isInfinite() || v.isNaN()) {
				throw new IllegalArgumentException("value is not finite");
			}
			return T_NUMBER;
		} else if (o instanceof Float) {
			//if (!Float.isFinite(((Float)o).floatValue())) {
			Float v = (Float) o;
			if (v.isInfinite() || v.isNaN()) {
				throw new IllegalArgumentException("value is not finite");
			}
			return T_NUMBER;
		} else if (o instanceof Byte || o instanceof Short || o instanceof BigInteger || o instanceof BigDecimal) {
			return T_NUMBER;
		}
		throw new IllegalArgumentException("Unknown type: " + o.getClass().toString());
	}

	public static int compare(Object o1, Object o2) {
		int t1 = getType(o1);
		int t2 = getType(o2);
		if (t1 != t2) {
			return t1 - t2;
		}
		switch (t1) {
			case T_UNDEFINED:
			case T_NULL:
			case T_FALSE:
			case T_TRUE:
				return 0;
			case T_NUMBER:
				if ((o1 instanceof Integer || o1 instanceof Long) && (o2 instanceof Integer || o2 instanceof Long)) {
					long l1 = ((Number)o1).longValue();
					long l2 = ((Number)o2).longValue();
					return l1 == l2 ? 0 : (l1 < l2 ? -1 : 1);
				}
				return getBig(o1).compareTo(getBig(o2));
			case T_STRING:
				return ((String)o1).compareTo((String) o2);
			case T_BIN:
				byte[] b1 = (byte[]) o1;
				byte[] b2 = (byte[]) o2;
				int msize = b1.length < b2.length ? b1.length : b2.length;
				for (int i = 0; i < msize; ++i) {
					if (b1[i] != b2[i]) {
						return b1[i] - b2[i];
					}
				}
				return b1.length - b2.length;
			case T_ARRAY:
				Iterator<?> i1 = getIt(o1);
				Iterator<?> i2 = getIt(o2);
				while (i1.hasNext() && i2.hasNext()) {
					int cmp = compare(i1.next(), i2.next());
					if (cmp != 0) {
						return cmp;
					}
				}
				return i1.hasNext() ? 1 : (i2.hasNext() ? -1 : 0);
			default:
				throw new RuntimeException("Unknown type: " + Integer.toString(t1));
		}
	}

	static BigDecimal getBig(Object o) {
		if (o instanceof Integer || o instanceof Long || o instanceof Short || o instanceof Byte) {
			return BigDecimal.valueOf(((Number)o).longValue());
		} else if (o instanceof BigDecimal) {
			return (BigDecimal) o;
		} else if (o instanceof BigInteger) {
			return new BigDecimal((BigInteger) o);
		} else if (o instanceof Float || o instanceof Double) {
			return BigDecimal.valueOf(((Number)o).doubleValue());
		}
		//throw new RuntimeException("Unsupported type: " + o.getClass().toString());
		return null;
	}

	static Thread startDaemonThread(Runnable r, String name) {
		Thread t = new Thread(r, name);
		t.setDaemon(true);
		t.start();
		return t;
	}
}
