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

package com.alipay.remoting.rpc;

import com.alipay.remoting.Connection;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.Protocol;
import com.alipay.remoting.ProtocolCode;
import com.alipay.remoting.ProtocolManager;
import com.alipay.remoting.RemotingContext;
import com.alipay.remoting.rpc.protocol.UserProcessor;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Dispatch messages to corresponding protocol.
 *
 * @author jiangping
 * @version $Id: RpcHandler.java, v 0.1 2015-12-14 PM4:01:37 tao Exp $
 */
@ChannelHandler.Sharable
public class RpcHandler extends ChannelInboundHandlerAdapter {

    private boolean serverSide;

    private ConcurrentHashMap<String, UserProcessor<?>> userProcessors;

    public RpcHandler() {
        serverSide = false;
    }

    public RpcHandler(ConcurrentHashMap<String, UserProcessor<?>> userProcessors) {
        serverSide = false;
        this.userProcessors = userProcessors;
    }

    public RpcHandler(boolean serverSide, ConcurrentHashMap<String, UserProcessor<?>> userProcessors) {
        this.serverSide = serverSide;
        this.userProcessors = userProcessors;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // 首先从连接的上下文中获取使用的协议的版本 ProtocolCode
        ProtocolCode protocolCode = ctx.channel().attr(Connection.PROTOCOL).get();
        // 根据ProtocolCode从ProtocolManager中获取具体的协议
        Protocol protocol = ProtocolManager.getProtocol(protocolCode);
        // 从协议中获取CommandHandler，并构造请求的上下文信息和请求的对象（代码片段中的msg）提交处理
        protocol.getCommandHandler().handleCommand(
                new RemotingContext(ctx, new InvokeContext(), serverSide, userProcessors), msg);
    }
}
