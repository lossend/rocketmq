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
# Build stage 1 `builder`:
# Compile RocketMQ from source and produce the binary distribution tarball.
#
# Maven builds distribution/target/rocketmq-<version>.tar.gz via the
# release-all profile.  Build tools stay in this stage and never reach the
# runtime image.
################################################################################
FROM eclipse-temurin:17-jdk AS builder

WORKDIR /src

RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        maven \
        git \
    ; \
    rm -rf /var/lib/apt/lists/*

# Copy POM files before source so the dependency-download layer is cached
# separately and only re-runs when POMs change.
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

RUN mvn dependency:go-offline -P release-all -q --no-transfer-progress 2>/dev/null || true

COPY . .

RUN mvn -Prelease-all -DskipTests clean package -q --no-transfer-progress

# Extract distribution.
# distribution/pom.xml: finalName=rocketmq-${version}, appendAssemblyId=false
#   → distribution/target/rocketmq-5.5.0.tar.gz
# includeBaseDirectory=true means the tarball root is rocketmq-5.5.0/,
# so --strip-components=1 lands the contents directly in /tmp/rocketmq-5.5.0/.
ARG version=5.5.0
RUN mkdir -p /tmp/rocketmq-${version} && \
    tar -xzf distribution/target/rocketmq-${version}.tar.gz \
        -C /tmp/rocketmq-${version} --strip-components=1

################################################################################
# Build stage 2:
# Make the actual RocketMQ docker image.
# Mirrors apache/rocketmq Dockerfile-ubuntu: same user, same script override
# pattern, same WORKDIR and ENTRYPOINT conventions.
################################################################################
FROM eclipse-temurin:17-jdk

ARG user=rocketmq
ARG group=rocketmq
ARG uid=3000
ARG gid=3000
ARG version=5.5.0

# Rocketmq version
ENV ROCKETMQ_VERSION=${version}

# Rocketmq home
ENV ROCKETMQ_HOME=/home/rocketmq/rocketmq-${ROCKETMQ_VERSION}

# expose namesrv port
EXPOSE 9876

# expose broker ports
EXPOSE 10909 10911 10912

# expose proxy ports
EXPOSE 8080 8081

# RocketMQ is run with user `rocketmq`, uid = 3000.
# If you bind mount a volume from the host or a data container,
# ensure you use the same uid.
RUN groupadd -g ${gid} ${group} \
    && useradd -l -u ${uid} -g ${gid} -m -s /bin/bash ${user} \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        less \
        openssl \
        which \
        numactl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy docker-specific scripts (docker-entrypoint.sh, runbroker-customize.sh,
# runserver-customize.sh) into bin/ — same approach as the official image.
COPY docker/scripts/ ${ROCKETMQ_HOME}/bin/

# Copy RocketMQ artifact from builder stage.
COPY --from=builder --chown=${uid}:${gid} /tmp/rocketmq-${version}/ ${ROCKETMQ_HOME}

# Override runserver.sh and runbroker.sh with docker-tuned variants
# (HEAP_OPTS override support, fixed /dev/shm GC log path).
# Fix java.ext.dirs in tools.sh so the admin CLI finds extra JARs.
# Set -Duser.home=/opt so logs/data don't land in /root.
RUN mv ${ROCKETMQ_HOME}/bin/runserver-customize.sh ${ROCKETMQ_HOME}/bin/runserver.sh \
 && mv ${ROCKETMQ_HOME}/bin/runbroker-customize.sh ${ROCKETMQ_HOME}/bin/runbroker.sh \
 && chmod -R a+x ${ROCKETMQ_HOME}/bin/ \
 && export JAVA_OPT=" -Duser.home=/opt" \
 && sed -i 's/${JAVA_HOME}\/jre\/lib\/ext/${JAVA_HOME}\/jre\/lib\/ext:${JAVA_HOME}\/lib\/ext/' ${ROCKETMQ_HOME}/bin/tools.sh \
 && chown -R ${uid}:${gid} ${ROCKETMQ_HOME}

USER ${user}

WORKDIR ${ROCKETMQ_HOME}/bin

ENTRYPOINT ["./docker-entrypoint.sh"]
# Dummy overridable parameter parsed by entrypoint.
# Pass "nameserver", "broker", "proxy", or "controller" to select the role,
# or set NODE_ROLE env var.
CMD ["dummy"]
