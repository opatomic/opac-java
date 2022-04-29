package com.opatomic;

import java.util.Iterator;

final class Request {
	final CharSequence command;
	final Iterator<?> args;
	final Object asyncId;
	final CallbackSF<Object,OpaRpcError> cb;

	Request(CharSequence command, Iterator<?> args, Object asyncId, CallbackSF<Object,OpaRpcError> cb) {
		this.command = command;
		this.args = args;
		this.asyncId = asyncId;
		this.cb = cb;
	}

	// TODO: implement toString()?

	// TODO: return an instance of this object from call() functions with a cancel() function: if request hasn't
	//  been sent then set flag and when it is time to serialize, call failure callback? see java.util.concurrent.Future
}
