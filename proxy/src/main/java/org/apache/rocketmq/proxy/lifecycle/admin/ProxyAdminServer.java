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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Loopback-friendly admin HTTP server built on the JDK's {@link HttpServer}. All
 * handlers are short and non-blocking: {@code POST /drain} creates or reuses a
 * drain run and returns {@code 202 + runId} immediately, so the fixed handler
 * threads can never be starved by waiters. {@code /state} and drain endpoints are
 * restricted to the real loopback peer, ignoring any forwarded headers.
 */
public final class ProxyAdminServer {

    private static final String JSON = "application/json; charset=utf-8";

    private final String bindAddress;
    private final int port;
    private final int backlog;
    private final ProxyAdminHandlers handlers;

    private volatile HttpServer httpServer;

    public ProxyAdminServer(String bindAddress, int port, int backlog, ProxyAdminHandlers handlers) {
        this.bindAddress = bindAddress;
        this.port = port;
        this.backlog = backlog;
        this.handlers = handlers;
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(bindAddress, port), backlog);
        server.createContext("/started", exchange -> respond(exchange, "GET", handlers::started));
        server.createContext("/live", exchange -> respond(exchange, "GET", handlers::live));
        server.createContext("/ready", exchange -> respond(exchange, "GET", handlers::ready));
        server.createContext("/ready-for-traffic",
            exchange -> respond(exchange, "GET", handlers::readyForTraffic));
        server.createContext("/state", loopbackOnly(exchange -> respond(exchange, "GET", handlers::state)));
        server.createContext("/drain", loopbackOnly(this::handleDrain));
        server.setExecutor(handlers.executor());
        server.start();
        this.httpServer = server;
    }

    public int boundPort() {
        HttpServer server = httpServer;
        return server == null ? port : server.getAddress().getPort();
    }

    public void stop(int delaySeconds) {
        HttpServer server = httpServer;
        if (server != null) {
            server.stop(delaySeconds);
        }
    }

    private void handleDrain(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        if ("POST".equals(method) && "/drain".equals(path)) {
            write(exchange, handlers.startDrain());
            return;
        }
        if ("GET".equals(method) && path.startsWith("/drain/")) {
            String runId = path.substring("/drain/".length());
            write(exchange, handlers.drainStatus(runId));
            return;
        }
        write(exchange, new ProxyAdminResponse(405, "{\"error\":\"method not allowed\"}"));
    }

    private void respond(HttpExchange exchange, String allowedMethod,
        java.util.function.Supplier<ProxyAdminResponse> supplier) throws IOException {
        if (!allowedMethod.equals(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().set("Allow", allowedMethod);
            write(exchange, new ProxyAdminResponse(405, "{\"error\":\"method not allowed\"}"));
            return;
        }
        write(exchange, supplier.get());
    }

    private HttpHandler loopbackOnly(HttpHandler delegate) {
        return exchange -> {
            if (!exchange.getRemoteAddress().getAddress().isLoopbackAddress()) {
                write(exchange, new ProxyAdminResponse(403, "{\"error\":\"loopback only\"}"));
                return;
            }
            delegate.handle(exchange);
        };
    }

    private void write(HttpExchange exchange, ProxyAdminResponse response) throws IOException {
        byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", JSON);
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(response.status(), body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    /** Handler surface, kept separate so it can be unit-tested without an HTTP server. */
    public interface ProxyAdminHandlers {
        java.util.concurrent.Executor executor();

        ProxyAdminResponse started();

        ProxyAdminResponse live();

        ProxyAdminResponse ready();

        ProxyAdminResponse readyForTraffic();

        ProxyAdminResponse state();

        ProxyAdminResponse startDrain();

        ProxyAdminResponse drainStatus(String runId);
    }
}
