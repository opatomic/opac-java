/*
 * Copyright 2018-2019 Opatomic
 * Open sourced with ISC license. Refer to LICENSE for details.
 */

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;

import com.opatomic.CallbackSF;
import com.opatomic.OpaDef;
import com.opatomic.OpaRpcError;
import com.opatomic.OpaStreamClient;
import com.opatomic.OpaUtils;
import com.opatomic.WaitCallbackSF;

public class Example {
	private static Iterator<Object> asIt(Object... objs) {
		return Arrays.asList(objs).iterator();
	}

	public static void main(String[] args) {

		CallbackSF<Object,OpaRpcError> echoResult = new CallbackSF<Object,OpaRpcError>() {
			@Override
			public void onSuccess(Object result) {
				System.out.println(OpaUtils.stringify(result));
			}
			@Override
			public void onFailure(OpaRpcError error) {
				System.out.println("ERROR: " + error.toString());
			}
		};

		try {
			WaitCallbackSF<Object,OpaRpcError> wcb = new WaitCallbackSF<Object,OpaRpcError>();
			Socket s = new Socket("127.0.0.1", 4567);
			s.setTcpNoDelay(true);
			OpaStreamClient c = new OpaStreamClient(s.getInputStream(), s.getOutputStream());

			c.call("PING", null, null);
			c.call("PING", null, echoResult);
			c.call("ECHO", asIt("Hello"), echoResult);
			c.call("ECHO", asIt(Arrays.asList(OpaDef.UndefinedObj, null, false, true, -1, 0, 1, OpaDef.EmptyBinObj, "string", OpaDef.EmptyArrayObj)), echoResult);

			c.call("INCR", asIt("TESTkey", -4), echoResult);
			c.call("INCR", asIt("TESTkey", new BigInteger("12345678901234567890")), echoResult);
			c.call("INCR", asIt("TESTkey", new BigDecimal("-1.0123456789")), echoResult);

			// note: SINTERSTORE is not yet implemented as an asyc op
			//  however, it potentially takes a long time and could eventually be performed without holding the db lock
			c.callA("SINTERSTORE", asIt("destkey", "set1", "set2"), echoResult);

			// the following commands will have errors
			c.call("ECHO", asIt("Hello", "ExtraArg!"), echoResult);
			c.call("BADCMD", asIt("Hello"), echoResult);
			c.call("SUBSCRIBE", asIt("badsub; needs async cb id (use callAP() instead)"), echoResult);

			final Object chid = c.callAP("SUBSCRIBE", asIt("channelName"), echoResult);
			c.call("PUBLISH", asIt("channelName", "chan message"), null);
			c.callA("UNSUBSCRIBE", asIt("channelName"), new CallbackSF<Object,OpaRpcError>() {
				@Override
				public void onSuccess(Object result) {
					System.out.println("unsubscribed");
					c.unregister(chid);
				}
				@Override
				public void onFailure(OpaRpcError error) {
					System.out.println("Error: could not unsubscribe; " + error.toString());
				}
			});

			c.call("QUIT", null, wcb.reset());
			wcb.waitIfNotDone();

			System.out.println("QUIT response received. close socket");
			s.close();

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
