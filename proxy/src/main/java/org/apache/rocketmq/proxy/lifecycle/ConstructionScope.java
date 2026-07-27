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

package org.apache.rocketmq.proxy.lifecycle;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Tracks resources as they are constructed so a construction failure rolls them
 * back in reverse order. On success {@link #commit()} transfers ownership to the
 * caller and the scope no longer closes anything. Every composite factory must
 * use its own nested scope, because an outer {@code own(name, buildX())} cannot
 * recover leaves allocated inside {@code buildX()} before it threw.
 */
public final class ConstructionScope implements AutoCloseable {

    /** Close action for a resource that does not implement {@link AutoCloseable}. */
    public interface CheckedCloseAction<T> {
        void close(T resource) throws Exception;
    }

    private static final class Owned {
        final String name;
        final Object resource;
        final CheckedCloseAction<Object> closer;

        Owned(String name, Object resource, CheckedCloseAction<Object> closer) {
            this.name = name;
            this.resource = resource;
            this.closer = closer;
        }
    }

    private final Deque<Owned> owned = new ArrayDeque<>();
    private boolean committed = false;

    public <T extends AutoCloseable> T own(String name, T resource) {
        return own(name, resource, AutoCloseable::close);
    }

    @SuppressWarnings("unchecked")
    public <T> T own(String name, T resource, CheckedCloseAction<T> closer) {
        if (resource != null) {
            owned.push(new Owned(name, resource, (CheckedCloseAction<Object>) closer));
        }
        return resource;
    }

    public void commit() {
        committed = true;
        owned.clear();
    }

    @Override
    public void close() {
        if (committed) {
            return;
        }
        rollback(null);
    }

    /**
     * Rolls back every owned resource in reverse order, attaching each close
     * failure as a suppressed exception to {@code primary} (or collecting them
     * into a new exception when {@code primary} is null and any close fails).
     */
    public void rollback(Throwable primary) {
        if (committed) {
            return;
        }
        Throwable aggregate = primary;
        while (!owned.isEmpty()) {
            Owned entry = owned.pop();
            try {
                entry.closer.close(entry.resource);
            } catch (Throwable closeError) {
                if (aggregate == null) {
                    aggregate = new RuntimeException("construction rollback failure");
                }
                aggregate.addSuppressed(closeError);
            }
        }
        if (primary == null && aggregate != null) {
            if (aggregate instanceof RuntimeException) {
                throw (RuntimeException) aggregate;
            }
            throw new RuntimeException(aggregate);
        }
    }
}
