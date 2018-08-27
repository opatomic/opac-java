package com.opatomic;

import java.io.IOException;
import java.io.OutputStream;


// see: lucene's UnicodeUtil.java
//    https://github.com/apache/lucene-solr/blob/master/lucene/core/src/java/org/apache/lucene/util/UnicodeUtil.java
// see https://issues.apache.org/jira/browse/THRIFT-765

/**
 * Utility functions to help convert from Java char[] to UTF-8 byte[]
 */
final class Utf8Utils {
	private static final int SURROGATE_OFFSET = Character.MIN_SUPPLEMENTARY_CODE_POINT - (0xD800 << 10) - 0xDC00;

	/**
	 * Determine how many bytes are required to encode chars as UTF-8 when calling 
	 * {@link #writeUtf8UsingBuffer(CharSequence,int,int,byte[],int,OutputStream) writeUtf8UsingBuffer()}. If a 
	 * surrogate pair is invalid, it is replaced with 3 byte substitution character.
	 * @param s the chars to count
	 * @param offset pos in {@code s}
	 * @param len number of chars to count
	 * @return number of bytes that are required to encode as UTF-8
	 */
	public static int getUtf8Len(final CharSequence s, final int offset, final int len) {
		final int end = offset + len;
		int numBytes = len;
		for (int i = offset; i < end; ++i) {
			int ch = s.charAt(i);
			if (ch < 0x80) {
			} else if (ch < 0x800) {
				++numBytes;
			} else if (ch < 0xD800 || ch > 0xDFFF) {
				numBytes += 2;
			} else {
				// surrogate pair
				// confirm valid high surrogate
				if (ch < 0xDC00 && (i < end - 1)) {
					int ch2 = s.charAt(i + 1);
					// confirm valid low surrogate
					if (ch2 >= 0xDC00 && ch2 <= 0xDFFF) {
						numBytes += 3;
						++i;
						continue;
					}
				}
				// invalid surrogate pair; will use 3 byte replacement when writing utf-8 bytes
				numBytes += 2;
			}
		}
		return numBytes;
	}

	/**
	 * Iteratively convert chars to UTF-8 bytes and write to a byte[] buffer, flushing to an OutputStream when buffer is full.
	 * If a surrogate pair is invalid, it is replaced with 3 bytes: 0xEF 0xBF 0xBD.
	 * @param s       chars to write
	 * @param offset  offset in {@code s}
	 * @param len     number of chars in s to write
	 * @param buff    where to write the UTF-8 bytes
	 * @param bpos    offset in {@code buff} to start writing
	 * @param out     flush buffer to this stream when full
	 * @return position in buffer after all bytes have been written
	 * @throws IOException
	 */
	public static int writeUtf8UsingBuffer(final CharSequence s, final int offset, final int len, byte[] buff, int bpos, OutputStream out) throws IOException {
		final int end = offset + len;
		int blen = buff.length;
		for (int i = offset; i < end; ++i) {
			if (bpos + 4 > blen) {
				out.write(buff, 0, bpos);
				bpos = 0;
			}
			int ch = s.charAt(i);
			if (ch < 0x80) {
				buff[bpos++] = (byte) ch;
			} else if (ch < 0x800) {
				buff[bpos++] = (byte) (0xC0 | (ch >> 6));
				buff[bpos++] = (byte) (0x80 | (ch & 0x3F));
			} else if (ch < 0xD800 || ch > 0xDFFF) {
				buff[bpos++] = (byte) (0xE0 | (ch >> 12));
				buff[bpos++] = (byte) (0x80 | ((ch >> 6) & 0x3F));
				buff[bpos++] = (byte) (0x80 | (ch & 0x3F));
			} else {
				// surrogate pair
				// confirm valid high surrogate
				if (ch < 0xDC00 && (i < end - 1)) {
					int ch2 = s.charAt(i + 1);
					// confirm valid low surrogate and write pair
					if (ch2 >= 0xDC00 && ch2 <= 0xDFFF) {
						ch2 = (ch << 10) + ch2 + SURROGATE_OFFSET;
						++i;
						buff[bpos++] = (byte) (0xF0 | (ch2 >> 18));
						buff[bpos++] = (byte) (0x80 | ((ch2 >> 12) & 0x3F));
						buff[bpos++] = (byte) (0x80 | ((ch2 >> 6) & 0x3F));
						buff[bpos++] = (byte) (0x80 | (ch2 & 0x3F));
						continue;
					}
				}
				// replace unpaired surrogate or out-of-order low surrogate with substitution character
				buff[bpos++] = (byte) (0xEF);
				buff[bpos++] = (byte) (0xBF);
				buff[bpos++] = (byte) (0xBD);
			}
		}
		return bpos;
	}

	private static void copyChunk(final CharSequence s, int offset, byte[] buff, int bpos, int len) {
		for (int end = bpos + len; bpos < end; ) {
			buff[bpos++] = (byte) s.charAt(offset++);
		}
	}

	/**
	 * Iteratively write ascii chars to a byte[] buffer, flushing to an OutputStream when buffer is full.
	 * If {@link #getUtf8Len(CharSequence,int,int) getUtf8Len()} returns a UTF-8 length that is 1 byte per char (same byte 
	 * count as char count) then chars are ascii and can be written using this function.
	 * @param s       chars to write
	 * @param offset  offset in {@code s}
	 * @param len     number of chars in s to write
	 * @param buff    where to write the UTF-8 bytes
	 * @param bpos    offset in {@code buff} to start writing
	 * @param out     flush buffer to this stream when full
	 * @return position in buffer after all bytes have been written
	 * @throws IOException
	 */
	public static int writeAsciiUsingBuffer(final CharSequence s, int offset, int len, byte[] buff, int bpos, OutputStream out) throws IOException {
		while (true) {
			int numToCopy = Math.min(len, buff.length - bpos);
			copyChunk(s, offset, buff, bpos, numToCopy);
			len -= numToCopy;
			if (len == 0) {
				return bpos + numToCopy;
			}
			out.write(buff, 0, buff.length);
			bpos = 0;
			offset += numToCopy;
		}
	}
}
