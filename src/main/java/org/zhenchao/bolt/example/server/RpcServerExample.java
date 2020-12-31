package org.zhenchao.bolt.example.server;

import com.alipay.remoting.ConnectionEventType;
import org.zhenchao.bolt.example.SimpleDisConnectionEventProcessor;

/**
 * @author zhenchao.wang 2020-12-23 16:33
 * @version 1.0.0
 */
public class RpcServerExample {

    public static void main(String[] args) {
        // 1. create a Rpc server with port assigned
        final BoltServer server = new BoltServer(8090);
        // 2. add processor for connect and close event if you need
        server.addConnectionEventProcessor(ConnectionEventType.CONNECT, new SimpleDisConnectionEventProcessor());
        server.addConnectionEventProcessor(ConnectionEventType.CLOSE, new SimpleDisConnectionEventProcessor());
        // 3. register user processor for client request
        server.registerUserProcessor(new SimpleServerUserProcessor());
        // 4. server start
        if (server.start()) {
            System.out.println("server start ok!");
        } else {
            System.out.println("server start failed!");
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.getRpcServer().shutdown()));
    }

}
