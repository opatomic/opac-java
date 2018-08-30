package com.opatomic;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Opatomic parser that parses objects from byte[] buffers in chunks.
 * <br/><br/>
 * Default object mapping:
 * <table>
 * <tr><th>Opatomic type</td><th>Java Object</th></tr>
 * <tr><td>undefined</td><td>OpaDef.UndefinedObj</td></tr>
 * <tr><td>null</td><td>null</td></tr>
 * <tr><td>false</td><td>Boolean.FALSE</td></tr>
 * <tr><td>true</td><td>Boolean.TRUE</td></tr>
 * <tr><td>numbers</td><td>Integer, Long, BigInteger, or BigDecimal</td></tr>
 * <tr><td>binary</td><td>byte[]</td></tr>
 * <tr><td>string</td><td>String</td></tr>
 * <tr><td>array</td><td>ArrayList</td></tr>
 * </table>
 */
public class OpaPartialParser {
	public static class ParseException extends RuntimeException {
		public ParseException(String msg) {
			super(msg);
		}
	}
	
	public static final class Buff {
		public byte[] data;
		public int idx;
		public int len;
	}
	
	public static final Object NOMORE = new Object();

	private static final Charset UTF8CS = Charset.forName("UTF-8");
	
	private static final byte S_NEXTOBJ = 1;
	private static final byte S_VARINT1 = 2;
	private static final byte S_VARINT2 = 3;
	private static final byte S_VARDEC1 = 4;
	private static final byte S_VARDEC2 = 5;
	private static final byte S_BIGINT  = 6;
	private static final byte S_BIGDEC1 = 7;
	private static final byte S_BIGDEC2 = 8;
	private static final byte S_BYTES1  = 9;
	private static final byte S_BYTES2  = 10;
	private static final byte S_BLOB    = 11;
	private static final byte S_STR     = 12;
	private static final byte S_ERR     = 13;

	
	private List<List<Object>> mContainers = new ArrayList<List<Object>>();
	private List<Object> mCurrCont;
	
	private int mState = S_NEXTOBJ; // type is int rather than byte because it is used often (int should have better performance than byte)
	private byte mNextState;
	private byte mNextState2;

	private int mVarintBitshift;
	private long mVarintLongVal;
	
	private int mDecExp;
	private int mObjType;

	private int mBytesIdx;
	private byte[] mBytes;
	

	private void throwErr(String msg) {
		mState = S_ERR;
		throw new ParseException(msg);
	}
	
	private void hitNext(Object o) {
		if (mCurrCont == null) {
			throwErr("no array container");
		}
		mCurrCont.add(o);
	}
	
	private void initVarint(int objType, byte nextState) {
		mState = S_VARINT1;
		mNextState = nextState;
		mObjType = objType;
		mVarintLongVal = 0;
		mVarintBitshift = 0;
	}
	
	private void initBytes(int objType, byte nextState) {
		initVarint(objType, S_BYTES1);
		mNextState2 = nextState;
	}
	
	private int getVarint32(boolean neg) {
		if (mVarintLongVal > Integer.MAX_VALUE) {
			throwErr("varint out of range");
		}
		return neg ? 0 - ((int)mVarintLongVal) : (int)mVarintLongVal;
	}

	private BigInteger bigIntFromVarint(boolean neg) {
		return BigInteger.valueOf(neg ? 0 - mVarintLongVal : mVarintLongVal);
	}
	
	private BigInteger bigIntFromBytes(boolean neg) {
		return new BigInteger(neg ? -1 : 1, mBytes);
	}
	
	private BigDecimal newBigDec(BigInteger man) {
		return new BigDecimal(man, 0 - mDecExp);
	}
	
	private String strFromBytes() {
		return new String(mBytes, UTF8CS);
	}
	
	/**
	 * Parse a buffer and return the next object encountered. Should continue calling this until
	 * {@link #NOMORE} is returned, indicating there's no more objects to parse in the buffer.
	 * @param b buffer containing the bytes to parse
	 * @return next Object encountered in buffer; or {@link #NOMORE} if buffer has no more objects
	 * @throws ParseException if data is malformed
	 */
	public Object parseNext(Buff b) {
		byte[] buff = b.data;
		int idx = b.idx;
		int stop = b.idx + b.len;
		MainLoop:
		while (true) {
			switch (mState) {
				case S_NEXTOBJ:
					if (idx >= stop) {
						b.idx = idx;
						b.len = 0;
						return NOMORE;
					}
					switch (buff[idx++]) {
						case OpaDef.C_UNDEFINED: hitNext(OpaDef.UndefinedObj);  continue;
						case OpaDef.C_NULL:      hitNext(null);                 continue;
						case OpaDef.C_FALSE:     hitNext(OpaDef.FalseObj);      continue;
						case OpaDef.C_TRUE:      hitNext(OpaDef.TrueObj);       continue;
						case OpaDef.C_ZERO:      hitNext(OpaDef.ZeroIntObj);    continue;
						case OpaDef.C_EMPTYBIN:  hitNext(OpaDef.EmptyBinObj);   continue;
						case OpaDef.C_EMPTYSTR:  hitNext(OpaDef.EmptyStrObj);   continue;
						case OpaDef.C_EMPTYLIST: hitNext(OpaDef.EmptyListObj);  continue;
						case OpaDef.C_SORTMAX:   hitNext(OpaDef.SortMaxObj);    continue;

						case OpaDef.C_NEGVARINT: initVarint(OpaDef.C_NEGVARINT, S_VARINT2); continue;
						case OpaDef.C_POSVARINT: initVarint(OpaDef.C_POSVARINT, S_VARINT2); continue;

						case OpaDef.C_NEGBIGINT: initBytes(OpaDef.C_NEGBIGINT, S_BIGINT); continue;
						case OpaDef.C_POSBIGINT: initBytes(OpaDef.C_POSBIGINT, S_BIGINT); continue;

						case OpaDef.C_POSPOSVARDEC: initVarint(OpaDef.C_POSPOSVARDEC, S_VARDEC1); continue;
						case OpaDef.C_POSNEGVARDEC: initVarint(OpaDef.C_POSNEGVARDEC, S_VARDEC1); continue;
						case OpaDef.C_NEGPOSVARDEC: initVarint(OpaDef.C_NEGPOSVARDEC, S_VARDEC1); continue;
						case OpaDef.C_NEGNEGVARDEC: initVarint(OpaDef.C_NEGNEGVARDEC, S_VARDEC1); continue;

						case OpaDef.C_POSPOSBIGDEC: initVarint(OpaDef.C_POSPOSBIGDEC, S_BIGDEC1); continue;
						case OpaDef.C_POSNEGBIGDEC: initVarint(OpaDef.C_POSNEGBIGDEC, S_BIGDEC1); continue;
						case OpaDef.C_NEGPOSBIGDEC: initVarint(OpaDef.C_NEGPOSBIGDEC, S_BIGDEC1); continue;
						case OpaDef.C_NEGNEGBIGDEC: initVarint(OpaDef.C_NEGNEGBIGDEC, S_BIGDEC1); continue;
						
						case OpaDef.C_BINLPVI: initBytes(OpaDef.C_BINLPVI, S_BLOB); continue;
						case OpaDef.C_STRLPVI: initBytes(OpaDef.C_STRLPVI, S_STR ); continue;
						
						case OpaDef.C_ARRAYSTART: {
							if (mCurrCont != null) {
								mContainers.add(mCurrCont);
							}
							mCurrCont = new ArrayList<Object>();
							continue;
						}
						case OpaDef.C_ARRAYEND: {
							if (mCurrCont == null) {
								throwErr("array end token when not in array");
							}
							if (mContainers.size() == 0) {
								List<Object> tmp = mCurrCont;
								mCurrCont = null;
								b.idx = idx;
								b.len = stop - idx;
								return tmp;
							}
							List<Object> parent = mContainers.remove(mContainers.size() - 1);
							parent.add(mCurrCont);
							mCurrCont = parent;
							continue;
						}
						default:
							throwErr("unknown char");
					}
					
				case S_VARINT1:
					while (true) {
						if (mVarintBitshift > 56) {
							// TODO: handle large varint as BigInteger?
							throwErr("varint too big");
						}
						if (idx >= stop) {
							b.idx = idx;
							b.len = 0;
							return NOMORE;
						}
						int bval = buff[idx++];
						mVarintLongVal |= ((long)(bval & 0x7F)) << mVarintBitshift;
						if ((bval & 0x80) == 0) {
							mState = mNextState;
							continue MainLoop;
						}
						mVarintBitshift += 7;
					}
				case S_VARINT2:
					hitNext(mObjType == OpaDef.C_NEGVARINT ? 0 - mVarintLongVal : mVarintLongVal);
					mState = S_NEXTOBJ;
					continue;
				case S_BYTES1:
					mBytes = new byte[getVarint32(false)];
					mBytesIdx = 0;
					mState = S_BYTES2;
					// fall-thru to next state
				case S_BYTES2: {
					int numToCopy = Math.min(stop - idx, mBytes.length - mBytesIdx);
					System.arraycopy(buff, idx, mBytes, mBytesIdx, numToCopy);
					mBytesIdx += numToCopy;
					idx += numToCopy;
					if (mBytesIdx < mBytes.length) {
						b.idx = idx;
						b.len = 0;
						return NOMORE;
					}
					mState = mNextState2;
					continue;
				}
				case S_BIGINT: {
					hitNext(bigIntFromBytes(mObjType == OpaDef.C_NEGBIGINT));
					mBytes = null;
					mState = S_NEXTOBJ;
					continue;
				}
					
				case S_VARDEC1: {
					mDecExp = getVarint32(mObjType == OpaDef.C_NEGPOSVARDEC || mObjType == OpaDef.C_NEGNEGVARDEC);
					initVarint(mObjType, S_VARDEC2);
					continue;
				}
				case S_VARDEC2: {
					hitNext(newBigDec(bigIntFromVarint(mObjType == OpaDef.C_POSNEGVARDEC || mObjType == OpaDef.C_NEGNEGVARDEC)));
					mState = S_NEXTOBJ;
					continue;
				}
					
				case S_BIGDEC1: {
					mDecExp = getVarint32(mObjType == OpaDef.C_NEGPOSBIGDEC || mObjType == OpaDef.C_NEGNEGBIGDEC);
					initBytes(mObjType, S_BIGDEC2);
					continue;
				}
				case S_BIGDEC2: {
					hitNext(newBigDec(bigIntFromBytes(mObjType == OpaDef.C_POSNEGBIGDEC || mObjType == OpaDef.C_NEGNEGBIGDEC)));
					mBytes = null;
					mState = S_NEXTOBJ;
					continue;
				}
					
				case S_BLOB: {
					hitNext(mBytes);
					mBytes = null;
					mState = S_NEXTOBJ;
					continue;
				}
				case S_STR: {
					hitNext(strFromBytes());
					mBytes = null;
					mState = S_NEXTOBJ;
					continue;
				}
					
				default:
					throwErr("unknown state");
			}
		}
	}
}
