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

package org.zhenchao.bolt.example.server;

import com.alipay.remoting.ConnectionEventProcessor;
import com.alipay.remoting.ConnectionEventType;
import com.alipay.remoting.rpc.RpcServer;
import com.alipay.remoting.rpc.protocol.UserProcessor;

/**
 * Demo for bolt server
 *
 * @author xiaomin.cxm
 * @version $Id: BoltServer.java, v 0.1 Apr 6, 2016 3:33:51 PM xiaomin.cxm Exp $
 */
public class BoltServer {

    /** port */
    private final int port;
    /** rpc server */
    private final RpcServer server;

    public BoltServer(int port) {
        this.port = port;
        this.server = new RpcServer(this.port);
    }

    public BoltServer(int port, boolean manageFeatureEnabled) {
        this.port = port;
        this.server = new RpcServer(this.port, manageFeatureEnabled);
    }

    public BoltServer(int port, boolean manageFeatureEnabled, boolean syncStop) {
        this.port = port;
        this.server = new RpcServer(this.port, manageFeatureEnabled, syncStop);
    }

    public boolean start() {
        this.server.startup();
        return true;
    }

    public void stop() {
        this.server.shutdown();
    }

    public RpcServer getRpcServer() {
        return this.server;
    }

    public void registerUserProcessor(UserProcessor<?> processor) {
        this.server.registerUserProcessor(processor);
    }

    public void addConnectionEventProcessor(ConnectionEventType type, ConnectionEventProcessor processor) {
        this.server.addConnectionEventProcessor(type, processor);
    }
}
