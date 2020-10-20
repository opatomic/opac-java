/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.opatomic;

import java.util.Collections;
import java.util.List;

public class OpaDef {
	static final boolean DEBUG = true;

	static final boolean BIGINT_BE = true;

	public static final int ERR_CLOSED      = -16394;
	public static final int ERR_INVRESPONSE = -16395;

	public static final byte C_UNDEFINED    = 'U';
	public static final byte C_NULL         = 'N';
	public static final byte C_FALSE        = 'F';
	public static final byte C_TRUE         = 'T';
	public static final byte C_ZERO         = 'O';
	public static final byte C_NEGINF       = 'P';
	public static final byte C_POSINF       = 'Q';
	public static final byte C_EMPTYBIN     = 'A';
	public static final byte C_EMPTYSTR     = 'R';
	public static final byte C_EMPTYARRAY   = 'M';
	public static final byte C_SORTMAX      = 'Z';

	public static final byte C_POSVARINT    = 'D';
	public static final byte C_NEGVARINT    = 'E';
	public static final byte C_POSPOSVARDEC = 'G';
	public static final byte C_POSNEGVARDEC = 'H';
	public static final byte C_NEGPOSVARDEC = 'I';
	public static final byte C_NEGNEGVARDEC = 'J';
	public static final byte C_POSBIGINT    = 'K';
	public static final byte C_NEGBIGINT    = 'L';
	public static final byte C_POSPOSBIGDEC = 'V';
	public static final byte C_POSNEGBIGDEC = 'W';
	public static final byte C_NEGPOSBIGDEC = 'X';
	public static final byte C_NEGNEGBIGDEC = 'Y';

	public static final byte C_BINLPVI      = 'B';
	public static final byte C_STRLPVI      = 'S';

	public static final byte C_ARRAYSTART   = '[';
	public static final byte C_ARRAYEND     = ']';

	public static final Object UndefinedObj = new Object() {
		@Override
		public String toString() {
			return "undefined";
		}
	};

	public static final Object SortMaxObj = new Object() {
		@Override
		public String toString() {
			return "SORTMAX";
		}
	};

	//public static final Object NullObj = new Object() {
	//	@Override
	//	public String toString() {
	//		return "null";
	//	}
	//};

	public static final Long ZeroObj       = Long.valueOf(0);
	public static final Double NegInfObj   = Double.valueOf(Double.NEGATIVE_INFINITY);
	public static final Double PosInfObj   = Double.valueOf(Double.POSITIVE_INFINITY);
	public static final byte[] EmptyBinObj = new byte[0];
	public static final String EmptyStrObj = "";
	public static final List<Object> EmptyArrayObj = Collections.emptyList();

	static void log(String msg) {
		System.out.println(msg);
	}
}
