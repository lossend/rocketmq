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

import io.grpc.Server;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.lifecycle.ShutdownDeadline;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrpcServerShutdownTest {

    private GrpcServer newServer(Server server) throws Exception {
        TlsCertificateManager tls = mock(TlsCertificateManager.class);
        return new GrpcServer(server, 30, TimeUnit.SECONDS, tls);
    }

    private ShutdownDeadline deadline(long budgetNanos) {
        return ShutdownDeadline.afterNanos(System.nanoTime(), budgetNanos, System::nanoTime);
    }

    @Test
    @DisplayName("initiateServerDrain calls server.shutdown exactly once")
    public void initiateOnceOnly() throws Exception {
        Server server = mock(Server.class);
        GrpcServer grpc = newServer(server);
        grpc.initiateServerDrain();
        grpc.initiateServerDrain();
        verify(server, times(1)).shutdown();
    }

    @Test
    @DisplayName("forceServerShutdown calls server.shutdownNow exactly once")
    public void forceOnceOnly() throws Exception {
        Server server = mock(Server.class);
        GrpcServer grpc = newServer(server);
        grpc.forceServerShutdown();
        grpc.forceServerShutdown();
        verify(server, times(1)).shutdownNow();
    }

    @Test
    @DisplayName("awaitServerTermination consumes the deadline's remaining time and returns the raw boolean")
    public void awaitConsumesDeadline() throws Exception {
        Server server = mock(Server.class);
        when(server.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        GrpcServer grpc = newServer(server);
        assertThat(grpc.awaitServerTermination(deadline(TimeUnit.SECONDS.toNanos(5)))).isTrue();
        verify(server).awaitTermination(anyLong(), any(TimeUnit.class));
    }

    @Test
    @DisplayName("an already-expired deadline does not block and reports the server's terminated state")
    public void expiredDeadlineNoBlock() throws Exception {
        Server server = mock(Server.class);
        when(server.isTerminated()).thenReturn(false);
        GrpcServer grpc = newServer(server);
        assertThat(grpc.awaitServerTermination(deadline(0))).isFalse();
        verify(server, never()).awaitTermination(anyLong(), any(TimeUnit.class));
        verify(server).isTerminated();
    }

    @Test
    @DisplayName("feature-off shutdown forces the server when graceful await times out")
    public void legacyShutdownForcesOnTimeout() throws Exception {
        Server server = mock(Server.class);
        when(server.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(false);
        when(server.isTerminated()).thenReturn(false);
        GrpcServer grpc = newServer(server);
        grpc.shutdown();
        verify(server).shutdown();
        verify(server).shutdownNow();
    }
}
