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

import com.alipay.remoting.config.ConfigManager;
import com.alipay.remoting.log.BoltLoggerFactory;
import com.alipay.remoting.util.RunStateRecordedFutureTask;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A default connection monitor that handle connections with strategies
 *
 * 定时重连，通过特定的 ConnectionMonitorStrategy 来对所有的链接池对象进行监控，内部维护了一个 ScheduledThreadPoolExecutor 来定时的执行 MonitorTask。
 * 在 SOFABolt 里 ConnectionMonitorStrategy 的实现是 ScheduledDisconnectStrategy 类，顾名思义，这是一个每次调度会执行关闭连接的监控策略，它的主要逻辑如下：
 *
 * - 通过filter方法来筛选出服务可用的连接和服务不可用的连接，并保存在两个List。
 * - 管理服务可用的连接，通过阈值 CONNECTION_THRESHOLD 来执行两种不同的逻辑
 * ----- 服务可用的连接数 > CONNECTION_THRESHOLD ：接数过多，需要释放资源，此时就会从这些可用链接里随机将一个配置成服务不可用的连接
 * ----- 服务的可用连接数 <= CONNECTION_THRESHOLD：连接数尚未占用过多的资源，只需取出上一次缓存在该集合中的“不可用”链接，然后执行closeFreshSelectConnections方法
 * - 关闭服务不可用的链接
 *
 * @author tsui
 * @version $Id: DefaultConnectionMonitor.java, v 0.1 2017-02-21 12:09 tsui Exp $
 */
public class DefaultConnectionMonitor extends AbstractLifeCycle {

    private static final Logger logger = BoltLoggerFactory.getLogger("CommonDefault");

    private final DefaultConnectionManager connectionManager;
    private final ConnectionMonitorStrategy strategy;

    private ScheduledThreadPoolExecutor executor;

    public DefaultConnectionMonitor(ConnectionMonitorStrategy strategy,
                                    DefaultConnectionManager connectionManager) {
        if (strategy == null) {
            throw new IllegalArgumentException("null strategy");
        }

        if (connectionManager == null) {
            throw new IllegalArgumentException("null connectionManager");
        }

        this.strategy = strategy;
        this.connectionManager = connectionManager;
    }

    @Override
    public void startup() throws LifeCycleException {
        super.startup();

        /* initial delay to execute schedule task, unit: ms */
        long initialDelay = ConfigManager.conn_monitor_initial_delay();

        /* period of schedule task, unit: ms*/
        long period = ConfigManager.conn_monitor_period();

        this.executor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory(
                "ConnectionMonitorThread", true), new ThreadPoolExecutor.AbortPolicy());
        this.executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    Map<String, RunStateRecordedFutureTask<ConnectionPool>> connPools = connectionManager
                            .getConnPools();
                    strategy.monitor(connPools);
                } catch (Exception e) {
                    logger.warn("MonitorTask error", e);
                }
            }
        }, initialDelay, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() throws LifeCycleException {
        super.shutdown();

        executor.purge();
        executor.shutdown();
    }

    /**
     * Start schedule task
     * please use {@link DefaultConnectionMonitor#startup()} instead
     */
    @Deprecated
    public void start() {
        startup();
    }

    /**
     * cancel task and shutdown executor
     * please use {@link DefaultConnectionMonitor#shutdown()} instead
     */
    @Deprecated
    public void destroy() {
        shutdown();
    }
}
