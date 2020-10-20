/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.opatomic;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;

/**
 * Buffered serializer that converts Objects to bytes to send via OutputStream.
 */
public final class OpaSerializer extends OutputStream {
	//private static final Charset    UTF8CS    = Charset.forName("UTF-8");
	private static final BigInteger BIMAXVARINT = BigInteger.valueOf(Long.MAX_VALUE);
	private static final BigInteger BIMINVARINT = BigInteger.valueOf(0L - Long.MAX_VALUE);
	private static final BigDecimal BDMAXVARINT = BigDecimal.valueOf(Long.MAX_VALUE);
	private static final BigDecimal BDMINVARINT = BigDecimal.valueOf(0L - Long.MAX_VALUE);

	/**
	 * An interface that indicates an Object knows how to serialize itself
	 */
	public interface OpaSerializable {
		/**
		 * Serialize this object to Opatomic format
		 * @param out where to write the objects's data
		 * @throws IOException
		 */
		public void writeOpaSO(OpaSerializer out) throws IOException;
	}


	private final OutputStream mOut;
	private final byte[] mBuff;
	private int mBuffPos = 0;

	/**
	 * Create a new serializer that will write to specified OutputStream.
	 * @param out Where to write values
	 * @param buffLen Length of internal buffer to reduce write() calls to stream.
	 */
	public OpaSerializer(OutputStream out, int buffLen) {
		if (buffLen <= 10) {
			throw new IllegalArgumentException("buffer len is too small");
		}
		mOut = out;
		mBuff = new byte[buffLen];
	}

	private void writeTypeAndVarint(int type, long val) throws IOException {
		ensureSpace(10);
		if (type != 0) {
			mBuff[mBuffPos++] = (byte) type;
		}
		while (val > 0x7F) {
			mBuff[mBuffPos++] = (byte) (0x80 | (val & 0x7F));
			val >>= 7;
		}
		mBuff[mBuffPos++] = (byte) (val & 0x7F);
	}

	private void writeTypeAndBigBytes(int type, BigInteger val) throws IOException {
		int bitLen = val.bitLength();
		int numBytes = (bitLen >> 3) + ((bitLen & 0x7) == 0 ? 0 : 1);

		byte[] buff = val.toByteArray();

		if (!(buff.length == numBytes || buff.length == numBytes + 1)) {
			throw new RuntimeException("BigInteger.toByteArray() returned unexpected value");
		}

		writeTypeAndVarint(type, numBytes);
		if (OpaDef.BIGINT_BE) {
			// may have to ignore a byte since byte array includes a sign bit
			write(buff, buff.length - numBytes, numBytes);
		} else {
			// byte array is in big-endian; reverse for little endian
			for (int i = 0; i < buff.length / 2; ++i) {
				byte tval = buff[i];
				buff[i] = buff[buff.length - i - 1];
				buff[buff.length - i - 1] = tval;
			}

			// may have to ignore a byte since byte array includes a sign bit
			write(buff, 0, numBytes);
		}
	}

	public void writeLong(long val) throws IOException {
		if (val == 0) {
			write(OpaDef.C_ZERO);
		} else if (val == Long.MIN_VALUE) {
			// TODO: use a hard coded byte array so there is no memory allocation here?
			writeBigInt(BigInteger.valueOf(val));
		} else {
			if (val < 0) {
				writeTypeAndVarint(OpaDef.C_NEGVARINT, 0 - val);
			} else {
				writeTypeAndVarint(OpaDef.C_POSVARINT, val);
			}
		}
	}

	public void writeBigInt(BigInteger v) throws IOException {
		int s = v.signum();
		if (s == 0) {
			write(OpaDef.C_ZERO);
		} else if (s > 0) {
			// positive value
			if (v.compareTo(BIMAXVARINT) <= 0) {
				// varint
				writeLong(v.longValue());
			} else {
				// bigint
				writeTypeAndBigBytes(OpaDef.C_POSBIGINT, v);
			}
		} else {
			// negative value
			if (v.compareTo(BIMINVARINT) >= 0) {
				// varint
				writeLong(v.longValue());
			} else {
				// bigint
				writeTypeAndBigBytes(OpaDef.C_NEGBIGINT, v.abs());
			}
		}
	}

	public void writeBigDec(BigDecimal v) throws IOException {
		int s = v.signum();
		if (s == 0) {
			write(OpaDef.C_ZERO);
			return;
		}
		int scale = v.scale();
		if (scale == 0) {
			// using some extra code here to avoid allocating unnecessary objects. avoid abs() and toBigIntegerExact() if not necessary
			if (s > 0) {
				if (v.compareTo(BDMAXVARINT) <= 0) {
					// pos varint
					writeLong(v.longValue());
				} else {
					// pos bigint
					writeTypeAndBigBytes(OpaDef.C_POSBIGINT, v.toBigIntegerExact());
				}
			} else {
				if (v.compareTo(BDMINVARINT) >= 0) {
					// neg varint
					writeLong(v.longValue());
				} else {
					// neg bigint
					writeTypeAndBigBytes(OpaDef.C_NEGBIGINT, v.toBigIntegerExact().abs());
				}
			}
		} else {
			BigInteger m = v.unscaledValue();

			boolean negExp = scale < 0 ? false : true;
			if (scale < 0) {
				scale = 0 - scale;
			}

			if (s < 0) {
				if (m.compareTo(BIMINVARINT) >= 0) {
					// neg vardec
					writeTypeAndVarint(negExp ? OpaDef.C_NEGNEGVARDEC : OpaDef.C_POSNEGVARDEC, scale);
					writeTypeAndVarint(0, 0 - m.longValue());
				} else {
					// neg bigdec
					writeTypeAndVarint(negExp ? OpaDef.C_NEGNEGBIGDEC : OpaDef.C_POSNEGBIGDEC, scale);
					writeTypeAndBigBytes(0, m.abs());
				}
			} else {
				if (m.compareTo(BIMAXVARINT) <= 0) {
					// pos vardec
					writeTypeAndVarint(negExp ? OpaDef.C_NEGPOSVARDEC : OpaDef.C_POSPOSVARDEC, scale);
					writeTypeAndVarint(0, m.longValue());
				} else {
					// pos bigdec
					writeTypeAndVarint(negExp ? OpaDef.C_NEGPOSBIGDEC : OpaDef.C_POSPOSBIGDEC, scale);
					writeTypeAndBigBytes(0, m);
				}
			}
		}
	}

	public void writeString(String s) throws IOException {
		int slen = s.length();
		if (slen == 0) {
			write(OpaDef.C_EMPTYSTR);
		} else {
			int utf8len = Utf8Utils.getUtf8Len(s, 0, slen);
			writeTypeAndVarint(OpaDef.C_STRLPVI, utf8len);
			if (utf8len == slen) {
				// string is ascii; no conversion necessary
				mBuffPos = Utf8Utils.writeAsciiUsingBuffer(s, 0, slen, mBuff, mBuffPos, mOut);
			} else {
				mBuffPos = Utf8Utils.writeUtf8UsingBuffer(s, 0, slen, mBuff, mBuffPos, mOut);
			}

			//byte[] bytes = s.getBytes(UTF8CS);
			//writeTypeAndVarint(OpaDef.STRLPVI, bytes.length);
			//write(bytes);
		}
	}

	public void writeBlob(byte[] b, int off, int len) throws IOException {
		if (len == 0) {
			write(OpaDef.C_EMPTYBIN);
		} else {
			writeTypeAndVarint(OpaDef.C_BINLPVI, len);
			write(b, off, len);
		}
	}

	public void writeArray(Iterator<?> i) throws IOException {
		if (i.hasNext()) {
			write(OpaDef.C_ARRAYSTART);
			do {
				writeObject(i.next());
			} while (i.hasNext());
			write(OpaDef.C_ARRAYEND);
		} else {
			write(OpaDef.C_EMPTYARRAY);
		}
	}

	public void writeObject(Object o) throws IOException {
		if (o == null) {
			write(OpaDef.C_NULL);
		} else if (o instanceof String) {
			writeString((String) o);
		} else if (o instanceof OpaSerializable) {
			((OpaSerializable)o).writeOpaSO(this);
		} else if (o instanceof Integer) {
			writeLong(((Integer)o).intValue());
		} else if (o instanceof Long) {
			writeLong(((Long)o).longValue());
		} else if (o instanceof Boolean) {
			write(((Boolean) o).booleanValue() ? OpaDef.C_TRUE : OpaDef.C_FALSE);
		} else if (o instanceof Iterable) {
			writeArray(((Iterable<?>)o).iterator());
		} else if (o instanceof byte[]) {
			byte[] b = (byte[]) o;
			writeBlob(b, 0, b.length);
		} else if (o instanceof Object[]) {
			Object[] a = (Object[]) o;
			if (a.length == 0) {
				write(OpaDef.C_EMPTYARRAY);
			} else {
				write(OpaDef.C_ARRAYSTART);
				for (int i = 0; i < a.length; ++i) {
					writeObject(a[i]);
				}
				write(OpaDef.C_ARRAYEND);
			}
		} else if (o instanceof BigInteger) {
			writeBigInt((BigInteger) o);
		} else if (o instanceof BigDecimal) {
			writeBigDec((BigDecimal) o);
		} else if (o instanceof Iterator) {
			writeArray((Iterator<?>) o);
		} else if (o == OpaDef.UndefinedObj) {
			write(OpaDef.C_UNDEFINED);
		} else if (o instanceof Float) {
			Float v = (Float) o;
			if (v.isInfinite()) {
				write(v.floatValue() == Float.NEGATIVE_INFINITY ? OpaDef.C_NEGINF : OpaDef.C_POSINF);
			} else {
				// TODO: use BigDecimal() constructor? or BigDecimal.valueOf()??
				//writeBigDec(new BigDecimal(((Float)o).doubleValue()));
				writeBigDec(BigDecimal.valueOf(v.doubleValue()));
			}
		} else if (o instanceof Double) {
			Double v = (Double) o;
			if (v.isInfinite()) {
				write(v.doubleValue() == Double.NEGATIVE_INFINITY ? OpaDef.C_NEGINF : OpaDef.C_POSINF);
			} else {
				// TODO: use BigDecimal() constructor? or BigDecimal.valueOf()??
				writeBigDec(BigDecimal.valueOf(v.doubleValue()));
			}
		} else if (o instanceof Short) {
			writeLong(((Short)o).intValue());
		} else if (o instanceof Byte) {
			writeLong(((Byte)o).intValue());
		} else if (o == OpaDef.SortMaxObj) {
			write(OpaDef.C_SORTMAX);
		} else {
			throw new RuntimeException("Unknown object " + o.getClass().getName());
		}
	}



	private void flushBuff() throws IOException {
		if (mBuffPos > 0) {
			mOut.write(mBuff, 0, mBuffPos);
			mBuffPos = 0;
		}
	}

	private void ensureSpace(int len) throws IOException {
		if (mBuffPos + len > mBuff.length) {
			flushBuff();
		}
	}

	@Override
	public void write(int v) throws IOException {
		if (mBuffPos >= mBuff.length) {
			flushBuff();
		}
		mBuff[mBuffPos++] = (byte) v;
	}

	@Override
	public void write(byte[] buff, int off, int len) throws IOException {
		if (len >= mBuff.length) {
			flushBuff();
			mOut.write(buff, off, len);
		} else {
			ensureSpace(len);
			System.arraycopy(buff, off, mBuff, mBuffPos, len);
			mBuffPos += len;
		}
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void flush() throws IOException {
		flushBuff();
		mOut.flush();
	}

	@Override
	public void close() throws IOException {
		try {
			flush();
		} catch (IOException e) {}
		mOut.close();
	}
}
