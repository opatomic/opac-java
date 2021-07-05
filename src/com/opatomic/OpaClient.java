/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

package com.opatomic;

import java.util.Iterator;

/**
 * Opatomic client interface used to send commands and receive the responses via callbacks. Unless
 * otherwise specified in implementation, the args parameters must not be modified until response
 * callback is invoked. ie, when call()/callA()/callID() returns, the request may not have been
 * serialized completely, meaning the args parameter might be in-use by serialization code.
 */
public interface OpaClient {
	/**
	 * Run specified command on server.
	 * @param cmd  Command to run
	 * @param args Command's parameters. Do not modify
	 * @param cb   Callback to invoke when response is received. If null then server will not send a response
	 */
	public void call(CharSequence cmd, Iterator<Object> args, CallbackSF<Object,OpaRpcError> cb);

	/**
	 * Run specified command on server with an auto-generated asynchronous id. Specified callback
	 * is invoked once, when response received. Response can arrive out of order. Command must respond
	 * only 1 time... do not use this for SUBSCRIBE (use callID() instead).
	 * @param cmd  Command to run
	 * @param args Command's parameters. Do not modify
	 * @param cb   Callback to invoke when response is received. Cannot be null.
	 */
	public void callA(CharSequence cmd, Iterator<Object> args, CallbackSF<Object,OpaRpcError> cb);

	/**
	 * Register a callback to an async id that can be used by callID().
	 * @param id  Async id
	 * @param cb  Callback to invoke when each response is received. Use null to remove registered callback
	 * @return    Previously registered callback for the specified async id.
	 */
	public CallbackSF<Object,OpaRpcError> registerCB(Object id, CallbackSF<Object,OpaRpcError> cb);

	/**
	 * Run specified command on the server with a specified async id. Any responses to the command
	 * will invoke the callback that was given as a parameter to register().
	 * @param id   Async id
	 * @param cmd  Command to run
	 * @param args Command's parameters. Do not modify
	 */
	public void callID(Object id, CharSequence cmd, Iterator<Object> args);

}
