package com.opatomic;

import java.util.Iterator;

/**
 * Opatomic client interface used to send commands and receive the responses via callbacks. Unless
 * otherwise specified in implementation, the args parameters must not be modified until response
 * callback is invoked. ie, when call()/callA()/callAP() returns, the request may not have been 
 * serialized completely, meaning the args parameter might be in-use by serialization code.
 *
 * @param <T> Object type for sending/receiving objects
 * @param <E> Error type for error responses
 */
public interface OpaClient<T,E> {
	/**
	 * Run specified command on server.
	 * @param cmd  Command to run
	 * @param args Command's parameters. Do not modify
	 * @param cb   Callback to invoke when response is received. If null then server will not send a response
	 */
	public void call(String cmd, Iterator<T> args, CallbackSF<T,E> cb);
	
	/**
	 * Run specified command on server with an auto-generated asynchronous id. Specified callback 
	 * is invoked once, when response received. Response can arrive out of order. Command must respond
	 * only 1 time... do not use this for SUBSCRIBE (use callAP() instead).
	 * @param cmd  Command to run
	 * @param args Command's parameters. Do not modify
	 * @param cb   Callback to invoke when response is received. Cannot be null.
	 */
	public void callA(String cmd, Iterator<T> args, CallbackSF<T,E> cb);
	
	/**
	 * Run specified command on server with an auto-generated asynchronous id. Specified callback
	 * is invoked every time a response is received. Command can respond any number of times. Use this
	 * for SUBSCRIBE.
	 * @param cmd  Command to run
	 * @param args Command's parameters. Do not modify
	 * @param sub  Callback to invoke when responses are received. Cannot be null.
	 * @return     An id that can be used to unregister() when done listening for responses to the command
	 */
	public Object callAP(String cmd, Iterator<T> args, CallbackSF<T,E> sub);
	
	/**
	 * Remove the callback for a callAP() invocation. Must call this when response from UNSUBSCRIBE 
	 * is received. Otherwise you will have a memory leak.
	 * @param id  The id returned from callAP()
	 * @return    true if async callback id was found and removed; false if id was not found
	 */
	public boolean unregister(Object id);
}
