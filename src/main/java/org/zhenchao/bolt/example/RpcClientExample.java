/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zhenchao.bolt.example;

import com.alipay.remoting.ConnectionEventType;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhenchao.bolt.example.server.RpcServerExample;

/**
 * a demo for rpc client, you can just run the main method after started rpc server of {@link RpcServerExample}
 *
 * @author tsui
 * @version $Id: RpcClientDemoByMain.java, v 0.1 2018-04-10 10:39 tsui Exp $
 */
public class RpcClientExample {

    static Logger logger = LoggerFactory.getLogger(RpcClientExample.class);

    static RpcClient client;

    static String addr = "127.0.0.1:8090";

    static {
        // 1. create a rpc client
        client = new RpcClient();
        // 2. add processor for connect and close event if you need
        client.addConnectionEventProcessor(ConnectionEventType.CONNECT, new SimpleConnectionEventProcessor());
        client.addConnectionEventProcessor(ConnectionEventType.CLOSE, new SimpleDisConnectionEventProcessor());
        // 3. do init
        client.startup();
    }

    public static void main(String[] args) {
        RequestBody req = new RequestBody(2, "hello world sync");
        try {
            String res = (String) client.invokeSync(addr, req, 3000);
            System.out.println("invoke sync result = [" + res + "]");
        } catch (RemotingException e) {
            String errMsg = "RemotingException caught in oneway!";
            logger.error(errMsg, e);
        } catch (InterruptedException e) {
            logger.error("interrupted!");
        } finally {
            client.shutdown();
        }
    }
}
