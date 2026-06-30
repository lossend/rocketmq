#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

################################################################################
# Stage 1 — source build
#
# Compiles RocketMQ from the local source tree and produces the binary
# distribution tarball at /build/rocketmq-dist.
#
# This stage is intentionally always re-run when source files change.
# The resulting dist directory is then COPYed into the final image, so the
# build tools (Maven, JDK compiler classes) do NOT end up in the shipped image.
################################################################################
FROM eclipse-temurin:17-jdk AS builder

WORKDIR /src

# Install Maven and build tooling.
# Cache the Maven wrapper download separately from the source copy so that
# rebuilds after a source-only change do not re-download Maven.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        maven \
        git \
    && rm -rf /var/lib/apt/lists/*

# Copy project descriptor files first so Docker layer-caching skips the full
# dependency download on source-only changes.
COPY pom.xml ./
COPY auth/pom.xml               auth/
COPY broker/pom.xml             broker/
COPY client/pom.xml             client/
COPY common/pom.xml             common/
COPY container/pom.xml          container/
COPY controller/pom.xml         controller/
COPY distribution/pom.xml       distribution/
COPY example/pom.xml            example/
COPY filter/pom.xml             filter/
COPY namesrv/pom.xml            namesrv/
COPY openmessaging/pom.xml      openmessaging/
COPY proxy/pom.xml              proxy/
COPY remoting/pom.xml           remoting/
COPY srvutil/pom.xml            srvutil/
COPY store/pom.xml              store/
COPY test/pom.xml               test/
COPY tieredstore/pom.xml        tieredstore/
COPY tools/pom.xml              tools/
COPY style/                     style/

# Pre-fetch all Maven dependencies (cached as a separate layer).
RUN mvn dependency:go-offline -P release-all -q --no-transfer-progress 2>/dev/null || true

# Copy full source and build the distribution.
COPY . .
RUN mvn -Prelease-all -DskipTests clean package -q --no-transfer-progress

# Extract the assembled distribution into a well-known path.
# finalName=rocketmq-${version} + appendAssemblyId=false → rocketmq-5.5.0.tar.gz
# includeBaseDirectory=true → tarball root is rocketmq-5.5.0/, hence --strip-components=1.
RUN mkdir -p /build/rocketmq-dist && \
    tar -xzf distribution/target/rocketmq-*.tar.gz -C /build/rocketmq-dist --strip-components=1

################################################################################
# Stage 2 — runtime image
#
# A single image that can run as nameserver, broker, proxy, or controller by
# passing the appropriate command (or setting NODE_ROLE).  Mirrors the approach
# used by the official apache/rocketmq image.
################################################################################
FROM eclipse-temurin:17-jdk

ARG user=rocketmq
ARG group=rocketmq
ARG uid=3000
ARG gid=3000
ARG version=5.5.0

ENV ROCKETMQ_VERSION=${version}
ENV ROCKETMQ_HOME=/home/rocketmq/rocketmq-${ROCKETMQ_VERSION}

# Expose standard RocketMQ ports:
#   9876  — nameserver
#   10909 — broker (VIP, fast channel)
#   10911 — broker (default)
#   10912 — broker (HAService)
#   8080  — proxy (gRPC / HTTP)
#   8081  — proxy (admin)
EXPOSE 9876 10909 10911 10912 8080 8081

# Create a non-root rocketmq user (uid 3000) matching the official image convention.
# Install minimal runtime utilities needed by the startup scripts.
RUN groupadd -g ${gid} ${group} && \
    useradd -l -u ${uid} -g ${gid} -m -s /bin/bash ${user} && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        less \
        openssl \
        which \
        numactl \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the assembled distribution produced by the builder stage.
COPY --from=builder --chown=${uid}:${gid} /build/rocketmq-dist/ ${ROCKETMQ_HOME}/

# Install the docker-specific entrypoint script.
COPY --chown=${uid}:${gid} docker/docker-entrypoint.sh ${ROCKETMQ_HOME}/bin/docker-entrypoint.sh

# Ensure all bin scripts are executable and fix up java.ext.dirs in tools.sh
# so the admin CLI can find extra JARs.
RUN chmod -R a+x ${ROCKETMQ_HOME}/bin/ && \
    sed -i 's|${JAVA_HOME}/jre/lib/ext|${JAVA_HOME}/jre/lib/ext:${JAVA_HOME}/lib/ext|' \
        ${ROCKETMQ_HOME}/bin/tools.sh 2>/dev/null || true && \
    chown -R ${uid}:${gid} ${ROCKETMQ_HOME}

USER ${user}

WORKDIR ${ROCKETMQ_HOME}/bin

ENTRYPOINT ["./docker-entrypoint.sh"]
# Default to nameserver; override with "broker", "proxy", or "controller".
CMD ["nameserver"]
