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

package org.apache.rocketmq.proxy.lifecycle.admin;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyAdminServerTest {

    private ProxyAdminServer server;
    private ExecutorService adminExecutor;
    private FakeHandlers handlers;

    @Before
    public void setUp() throws Exception {
        adminExecutor = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS,
            new java.util.concurrent.ArrayBlockingQueue<>(16));
        handlers = new FakeHandlers(adminExecutor);
        server = new ProxyAdminServer("127.0.0.1", 0, 0, handlers);
        server.start();
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop(0);
        }
        if (adminExecutor != null) {
            adminExecutor.shutdownNow();
        }
    }

    private int[] get(String path) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(
            "http://127.0.0.1:" + server.boundPort() + path).openConnection();
        conn.setRequestMethod("GET");
        int code = conn.getResponseCode();
        drain(conn);
        return new int[] {code};
    }

    private String post(String path) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(
            "http://127.0.0.1:" + server.boundPort() + path).openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.getOutputStream().close();
        int code = conn.getResponseCode();
        String body = readBody(conn);
        return code + ":" + body;
    }

    private static void drain(HttpURLConnection conn) throws Exception {
        readBody(conn);
    }

    private static String readBody(HttpURLConnection conn) throws Exception {
        InputStream is = conn.getResponseCode() < 400 ? conn.getInputStream() : conn.getErrorStream();
        if (is == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }

    @Test
    @DisplayName("health endpoints return the handler-provided status codes")
    public void healthEndpoints() throws Exception {
        assertThat(get("/started")[0]).isEqualTo(200);
        assertThat(get("/live")[0]).isEqualTo(200);
        assertThat(get("/ready")[0]).isEqualTo(200);
        assertThat(get("/ready-for-traffic")[0]).isEqualTo(200);
    }

    @Test
    @DisplayName("POST /drain returns 202 with a runId immediately and is idempotent on re-entry")
    public void postDrainReturns202() throws Exception {
        String first = post("/drain");
        assertThat(first).startsWith("202:");
        assertThat(first).contains("drain-1");
        String second = post("/drain");
        assertThat(second).startsWith("202:");
        // same run id on re-entry
        assertThat(second).contains("drain-1");
        assertThat(handlers.drainStarts.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("under a POST /drain flood the /live handler still responds quickly")
    public void livenessNotStarvedUnderDrainFlood() throws Exception {
        int floods = 40;
        ExecutorService clients = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < floods; i++) {
            clients.submit(() -> {
                try {
                    start.await();
                    post("/drain");
                } catch (Exception ignored) {
                }
            });
        }
        start.countDown();
        long begin = System.nanoTime();
        assertThat(get("/live")[0]).isEqualTo(200);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
        clients.shutdown();
        clients.awaitTermination(30, TimeUnit.SECONDS);
        assertThat(elapsedMs).isLessThan(5000);
    }

    private static final class FakeHandlers implements ProxyAdminServer.ProxyAdminHandlers {
        private final Executor executor;
        final AtomicInteger drainStarts = new AtomicInteger();

        FakeHandlers(Executor executor) {
            this.executor = executor;
        }

        @Override
        public Executor executor() {
            return executor;
        }

        @Override
        public ProxyAdminResponse started() {
            return new ProxyAdminResponse(200, "{\"status\":\"UP\"}");
        }

        @Override
        public ProxyAdminResponse live() {
            return new ProxyAdminResponse(200, "{\"status\":\"UP\"}");
        }

        @Override
        public ProxyAdminResponse ready() {
            return new ProxyAdminResponse(200, "{\"status\":\"UP\"}");
        }

        @Override
        public ProxyAdminResponse readyForTraffic() {
            return new ProxyAdminResponse(200, "{\"status\":\"UP\"}");
        }

        @Override
        public ProxyAdminResponse state() {
            return new ProxyAdminResponse(200, "{\"state\":\"READY\"}");
        }

        @Override
        public ProxyAdminResponse startDrain() {
            drainStarts.incrementAndGet();
            return new ProxyAdminResponse(202, "{\"runId\":\"drain-1\"}");
        }

        @Override
        public ProxyAdminResponse drainStatus(String runId) {
            return new ProxyAdminResponse(200, "{\"runId\":\"" + runId + "\",\"phase\":\"DRAINING\"}");
        }
    }
}
