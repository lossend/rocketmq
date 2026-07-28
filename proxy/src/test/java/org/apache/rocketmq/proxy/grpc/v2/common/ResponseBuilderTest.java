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

package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.Code;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ResponseBuilderTest {

    private final ResponseBuilder responseBuilder = new ResponseBuilder();

    @Test
    @DisplayName("an exceptional gRPC status can never be mapped to Code.OK")
    public void grpcExceptionCannotMapToOk() {
        assertThat(responseBuilder.buildStatus(new GrpcProxyException(Code.OK, "invalid success exception"))
            .getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
    }

    @Test
    @DisplayName("an exceptional Broker response can never be mapped to Code.OK")
    public void brokerExceptionCannotMapToOk() {
        assertThat(responseBuilder.buildStatus(new MQBrokerException(ResponseCode.SUCCESS, "invalid success exception"))
            .getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
    }

    @Test
    @DisplayName("an exceptional client response can never be mapped to Code.OK")
    public void clientExceptionCannotMapToOk() {
        assertThat(responseBuilder.buildStatus(new MQClientException(ResponseCode.SUCCESS, "invalid success exception"))
            .getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
    }
}
