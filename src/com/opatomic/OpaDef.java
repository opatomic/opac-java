package com.opatomic;

import java.util.Collections;
import java.util.List;

public class OpaDef {
	public static final boolean DEBUG = true;
	
	public static final boolean BIGINT_BE = true;
	
	public static final byte C_UNDEFINED    = 'U';
	public static final byte C_NULL         = 'N';
	public static final byte C_FALSE        = 'F';
	public static final byte C_TRUE         = 'T';
	public static final byte C_ZERO         = 'O';
	public static final byte C_EMPTYBIN     = 'A';
	public static final byte C_EMPTYSTR     = 'R';
	public static final byte C_EMPTYLIST    = 'M';
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
	
	//public static final Object NullObj = new Object() {
	//	@Override
	//	public String toString() {
	//		return "null";
	//	}
	//};

	public static final Boolean FalseObj   = Boolean.FALSE;
	public static final Boolean TrueObj    = Boolean.TRUE;
	public static final Integer ZeroIntObj = Integer.valueOf(0);
	public static final byte[] EmptyBinObj = new byte[0];
	public static final String EmptyStrObj = "";
	public static final List<Object> EmptyListObj = Collections.emptyList();
	
	public static final void log(String msg) {
		System.out.println(msg);
	}
}