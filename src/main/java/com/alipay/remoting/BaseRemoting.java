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

package com.alipay.remoting;

import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.log.BoltLoggerFactory;
import com.alipay.remoting.util.RemotingUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Base remoting capability.
 *
 * @author jiangping
 * @version $Id: BaseRemoting.java, v 0.1 Mar 4, 2016 12:09:56 AM tao Exp $
 */
public abstract class BaseRemoting {
    /** logger */
    private static final Logger logger = BoltLoggerFactory.getLogger("CommonDefault");

    protected CommandFactory commandFactory;

    public BaseRemoting(CommandFactory commandFactory) {
        this.commandFactory = commandFactory;
    }

    /**
     * Synchronous invocation
     *
     * @param conn
     * @param request
     * @param timeoutMillis
     * @return
     * @throws InterruptedException
     * @throws RemotingException
     */
    protected RemotingCommand invokeSync(final Connection conn, final RemotingCommand request, final int timeoutMillis)
            throws RemotingException, InterruptedException {
        // 创建InvokeFuture
        final InvokeFuture future = createInvokeFuture(request, request.getInvokeContext());
        conn.addInvokeFuture(future);
        final int requestId = request.getId();
        try {
            // 在Netty的ChannelFuture中添加Listener，在写入操作失败的情况下通过future.putResponse方法修改Future状态
            // （正常服务端响应也是通过future.putResponse来改变InvokeFuture的状态的，这个流程不展开说明）
            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (!f.isSuccess()) {
                        conn.removeInvokeFuture(requestId);
                        future.putResponse(commandFactory.createSendFailedResponse(
                                conn.getRemoteAddress(), f.cause()));
                        logger.error("Invoke send failed, id={}", requestId, f.cause());
                    }
                }

            });
        } catch (Exception e) {
            conn.removeInvokeFuture(requestId);
            // 写入出现异常的情况下也是通过future.putResponse方法修改Future状态
            future.putResponse(commandFactory.createSendFailedResponse(conn.getRemoteAddress(), e));
            logger.error("Exception caught when sending invocation, id={}", requestId, e);
        }
        // 通过future.waitResponse来执行等待响应 其中和超时相关的是future.waitResponse的调用，
        // InvokeFuture内部通过java.util.concurrent.CountDownLatch来实现超时触发。
        RemotingCommand response = future.waitResponse(timeoutMillis);

        if (response == null) {
            conn.removeInvokeFuture(requestId);
            response = this.commandFactory.createTimeoutResponse(conn.getRemoteAddress());
            logger.warn("Wait response, request id={} timeout!", requestId);
        }

        return response;
    }

    /**
     * Invocation with callback.
     *
     * @param conn
     * @param request
     * @param invokeCallback
     * @param timeoutMillis
     * @throws InterruptedException
     */
    protected void invokeWithCallback(final Connection conn, final RemotingCommand request,
                                      final InvokeCallback invokeCallback, final int timeoutMillis) {
        // 创建InvokeFuture
        final InvokeFuture future = createInvokeFuture(conn, request, request.getInvokeContext(), invokeCallback);
        conn.addInvokeFuture(future);
        final int requestId = request.getId();
        try {
            // 创建Timeout实例，Timeout实例的run方法中通过future.putResponse来修改InvokeFuture的状态
            Timeout timeout = TimerHolder.getTimer().newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    InvokeFuture future = conn.removeInvokeFuture(requestId);
                    if (future != null) {
                        future.putResponse(commandFactory.createTimeoutResponse(conn
                                .getRemoteAddress()));
                        future.tryAsyncExecuteInvokeCallbackAbnormally();
                    }
                }

            }, timeoutMillis, TimeUnit.MILLISECONDS);
            future.addTimeout(timeout);
            // 在Netty的ChannelFuture中添加Listener，在写入操作失败的情况下通过future.cancelTimeout来取消超时任务，
            // 通过future.putResponse来修改InvokeFuture的状态
            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture cf) throws Exception {
                    if (!cf.isSuccess()) {
                        InvokeFuture f = conn.removeInvokeFuture(requestId);
                        if (f != null) {
                            f.cancelTimeout();
                            f.putResponse(commandFactory.createSendFailedResponse(
                                    conn.getRemoteAddress(), cf.cause()));
                            f.tryAsyncExecuteInvokeCallbackAbnormally();
                        }
                        logger.error("Invoke send failed. The address is {}",
                                RemotingUtil.parseRemoteAddress(conn.getChannel()), cf.cause());
                    }
                }

            });
        } catch (Exception e) {
            /*
             * 在写入异常的情况下同样通过future.cancelTimeout来取消超时任务，通过future.putResponse来修改InvokeFuture的状态
             *
             * 在异步调用的实现中，通过Timeout来触发超时任务，相当于同步调用中的CountDownLatch#await(timeout, timeoutUnit)。
             * Future#cancelTimeout()方法则是调用了Timeout的cancel来取消超时任务，相当于同步调用中通过 CountDownLatch#countDown() 来提前结束超时任务。
             * 具体超时任务的管理则全部委托给了Netty的Timer实现。 另外值得注意的一点是SOFABolt在使用Netty的Timer时采用了单例的模式，
             * 因为一般情况下使用一个Timer管理所有的超时任务即可，这样可以节省系统的开销。
             */
            InvokeFuture f = conn.removeInvokeFuture(requestId);
            if (f != null) {
                f.cancelTimeout();
                f.putResponse(commandFactory.createSendFailedResponse(conn.getRemoteAddress(), e));
                f.tryAsyncExecuteInvokeCallbackAbnormally();
            }
            logger.error("Exception caught when sending invocation. The address is {}",
                    RemotingUtil.parseRemoteAddress(conn.getChannel()), e);
        }
    }

    /**
     * Invocation with future returned.
     *
     * @param conn
     * @param request
     * @param timeoutMillis
     * @return
     */
    protected InvokeFuture invokeWithFuture(final Connection conn, final RemotingCommand request,
                                            final int timeoutMillis) {

        final InvokeFuture future = createInvokeFuture(request, request.getInvokeContext());
        conn.addInvokeFuture(future);
        final int requestId = request.getId();
        try {
            Timeout timeout = TimerHolder.getTimer().newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    InvokeFuture future = conn.removeInvokeFuture(requestId);
                    if (future != null) {
                        future.putResponse(commandFactory.createTimeoutResponse(conn
                                .getRemoteAddress()));
                    }
                }

            }, timeoutMillis, TimeUnit.MILLISECONDS);
            future.addTimeout(timeout);

            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture cf) throws Exception {
                    if (!cf.isSuccess()) {
                        InvokeFuture f = conn.removeInvokeFuture(requestId);
                        if (f != null) {
                            f.cancelTimeout();
                            f.putResponse(commandFactory.createSendFailedResponse(
                                    conn.getRemoteAddress(), cf.cause()));
                        }
                        logger.error("Invoke send failed. The address is {}",
                                RemotingUtil.parseRemoteAddress(conn.getChannel()), cf.cause());
                    }
                }

            });
        } catch (Exception e) {
            InvokeFuture f = conn.removeInvokeFuture(requestId);
            if (f != null) {
                f.cancelTimeout();
                f.putResponse(commandFactory.createSendFailedResponse(conn.getRemoteAddress(), e));
            }
            logger.error("Exception caught when sending invocation. The address is {}",
                    RemotingUtil.parseRemoteAddress(conn.getChannel()), e);
        }
        return future;
    }

    /**
     * Oneway invocation.
     *
     * @param conn
     * @param request
     * @throws InterruptedException
     */
    protected void oneway(final Connection conn, final RemotingCommand request) {
        try {
            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (!f.isSuccess()) {
                        logger.error("Invoke send failed. The address is {}",
                                RemotingUtil.parseRemoteAddress(conn.getChannel()), f.cause());
                    }
                }

            });
        } catch (Exception e) {
            if (null == conn) {
                logger.error("Conn is null");
            } else {
                logger.error("Exception caught when sending invocation. The address is {}",
                        RemotingUtil.parseRemoteAddress(conn.getChannel()), e);
            }
        }
    }

    /**
     * Create invoke future with {@link InvokeContext}.
     *
     * @param request
     * @param invokeContext
     * @return
     */
    protected abstract InvokeFuture createInvokeFuture(final RemotingCommand request,
                                                       final InvokeContext invokeContext);

    /**
     * Create invoke future with {@link InvokeContext}.
     *
     * @param conn
     * @param request
     * @param invokeContext
     * @param invokeCallback
     * @return
     */
    protected abstract InvokeFuture createInvokeFuture(final Connection conn,
                                                       final RemotingCommand request,
                                                       final InvokeContext invokeContext,
                                                       final InvokeCallback invokeCallback);

    protected CommandFactory getCommandFactory() {
        return commandFactory;
    }
}
