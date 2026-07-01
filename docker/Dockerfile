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
# Build with JDK 8 (not 17): the pom uses source/target 1.8 without --release,
# so a JDK 17 compiler would link ByteBuffer.position(int) against its Java 9+
# covariant override and produce bytecode the JDK 8 runtime cannot load
# (NoSuchMethodError).  This mirrors the official dist-tar job
# (snapshot-automation.yml): JDK 8 + Maven, same `mvn -Prelease-all ...` cmd.
# `maven:3.9-eclipse-temurin-8` ships JDK 8 + Maven, so no apt-get is needed.
FROM public.ecr.aws/docker/library/maven:3.9-eclipse-temurin-8 AS builder

WORKDIR /src

COPY . .

RUN mvn -Prelease-all -DskipTests -Dspotbugs.skip=true clean install

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
FROM public.ecr.aws/docker/library/eclipse-temurin:8-jdk

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

# RocketMQ is run with user `rocketmq`, uid = 3000
# If you bind mount a volume from the host or a data container,
# ensure you use the same uid
RUN groupadd -g ${gid} ${group} \
    && useradd -l -u ${uid} -g ${gid} -m -s /bin/bash ${user} \
    && apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        less \
        openssl \
        which \
    \
    && apt-get clean; \
    rm -rf /var/lib/apt/lists/*

# Copy customized scripts
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
