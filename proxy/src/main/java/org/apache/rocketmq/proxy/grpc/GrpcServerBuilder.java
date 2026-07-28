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
package org.apache.rocketmq.proxy.grpc;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerStreamTracer;
import io.grpc.ServerTransportFilter;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.GlobalExceptionInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;

public class GrpcServerBuilder {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected NettyServerBuilder serverBuilder;

    protected long time = 30;

    protected TimeUnit unit = TimeUnit.SECONDS;

    protected TlsCertificateManager tlsCertificateManager;

    public static GrpcServerBuilder newBuilder(ThreadPoolExecutor executor, int port,
        TlsCertificateManager tlsCertificateManager) {
        return new GrpcServerBuilder(executor, port, tlsCertificateManager);
    }

    protected GrpcServerBuilder(ThreadPoolExecutor executor, int port, TlsCertificateManager tlsCertificateManager) {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        this.tlsCertificateManager = tlsCertificateManager;
        serverBuilder = NettyServerBuilder.forPort(port)
            .maxConcurrentCallsPerConnection(config.getGrpcMaxConcurrentCallsPerConnection());

        serverBuilder.protocolNegotiator(new ProxyAndTlsProtocolNegotiator());

        // build server
        int bossLoopNum = config.getGrpcBossLoopNum();
        int workerLoopNum = config.getGrpcWorkerLoopNum();
        int maxInboundMessageSize = config.getGrpcMaxInboundMessageSize();
        long idleTimeMills = config.getGrpcClientIdleTimeMills();

        if (config.isEnableGrpcEpoll()) {
            serverBuilder.bossEventLoopGroup(new EpollEventLoopGroup(bossLoopNum))
                .workerEventLoopGroup(new EpollEventLoopGroup(workerLoopNum))
                .channelType(EpollServerSocketChannel.class)
                .executor(executor);
        } else {
            serverBuilder.bossEventLoopGroup(new NioEventLoopGroup(bossLoopNum))
                .workerEventLoopGroup(new NioEventLoopGroup(workerLoopNum))
                .channelType(NioServerSocketChannel.class)
                .executor(executor);
        }

        serverBuilder.maxInboundMessageSize(maxInboundMessageSize)
            .maxConnectionIdle(idleTimeMills, TimeUnit.MILLISECONDS);

        log.info("grpc server has built. port: {}, bossLoopNum: {}, workerLoopNum: {}, maxInboundMessageSize: {}",
            port, bossLoopNum, workerLoopNum, maxInboundMessageSize);
    }

    GrpcServerBuilder(NettyServerBuilder serverBuilder) {
        this.serverBuilder = serverBuilder;
    }

    public GrpcServerBuilder shutdownTime(long time, TimeUnit unit) {
        this.time = time;
        this.unit = unit;
        return this;
    }

    public GrpcServerBuilder addService(BindableService service) {
        this.serverBuilder.addService(service);
        return this;
    }

    public GrpcServerBuilder addService(ServerServiceDefinition service) {
        this.serverBuilder.addService(service);
        return this;
    }

    public GrpcServerBuilder appendInterceptor(ServerInterceptor interceptor) {
        this.serverBuilder.intercept(interceptor);
        return this;
    }

    public GrpcServer build() throws Exception {
        return new GrpcServer(this.serverBuilder.build(), time, unit, tlsCertificateManager);
    }

    public GrpcServerBuilder configInterceptor() {
        this.serverBuilder
            .intercept(new GlobalExceptionInterceptor())
            .intercept(new ContextInterceptor())
            .intercept(new HeaderInterceptor());
        return this;
    }

    /**
     * Preserves the original lifecycle-builder contract, which installed send
     * draining together with the transport lifecycle.
     */
    public GrpcServerBuilder configLifecycle(ServerStreamTracer.Factory sendTracerFactory,
        ServerInterceptor sendInterceptor, ServerInterceptor activeCallInterceptor,
        ServerTransportFilter transportFilter, long connectionAgeSeconds, long connectionAgeGraceSeconds) {
        return configLifecycle(sendTracerFactory, sendInterceptor, activeCallInterceptor,
            transportFilter, connectionAgeSeconds, connectionAgeGraceSeconds, true);
    }

    /**
     * Installs the graceful-lifecycle wiring when enabled: connection max-age (so
     * long-lived streams rotate), the transport lifecycle filter, and the
     * active-call interceptor that wraps every non-unary call. The send stream
     * tracer and send-permit interceptor (the SendPermit accounting) are only
     * installed when {@code installSendDrain} is true, so that accounting can be
     * disabled independently of the rest of the lifecycle. Interceptors are applied
     * outermost-first; the send interceptor reads the tracer holder from the Context.
     *
     * @param sendTracerFactory     per-stream send tracer; installed only when {@code installSendDrain}
     * @param sendInterceptor       send-permit admission interceptor; installed only when {@code installSendDrain}
     * @param activeCallInterceptor non-unary active-call interceptor; always installed
     * @param transportFilter       transport lifecycle filter; always installed
     * @param connectionAgeSeconds  max connection age before graceful rotation
     * @param connectionAgeGraceSeconds grace period after max connection age
     * @param installSendDrain      whether to install the SendPermit accounting components
     * @return this builder
     */
    public GrpcServerBuilder configLifecycle(ServerStreamTracer.Factory sendTracerFactory,
        ServerInterceptor sendInterceptor, ServerInterceptor activeCallInterceptor,
        ServerTransportFilter transportFilter, long connectionAgeSeconds, long connectionAgeGraceSeconds,
        boolean installSendDrain) {
        this.serverBuilder
            .maxConnectionAge(connectionAgeSeconds, TimeUnit.SECONDS)
            .maxConnectionAgeGrace(connectionAgeGraceSeconds, TimeUnit.SECONDS)
            .addTransportFilter(transportFilter)
            .intercept(activeCallInterceptor);
        if (installSendDrain) {
            this.serverBuilder
                .addStreamTracerFactory(sendTracerFactory)
                .intercept(sendInterceptor);
        }
        return this;
    }
}
