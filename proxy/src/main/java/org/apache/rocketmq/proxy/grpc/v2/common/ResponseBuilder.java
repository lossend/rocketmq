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
import apache.rocketmq.v2.Status;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.proxy.service.route.TopicRouteHelper;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class ResponseBuilder {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected static final Map<Integer, Code> RESPONSE_CODE_MAPPING = new ConcurrentHashMap<>();

    protected static final Object INSTANCE_CREATE_LOCK = new Object();
    protected static volatile ResponseBuilder instance;

    static {
        RESPONSE_CODE_MAPPING.put(ResponseCode.SUCCESS, Code.OK);
        RESPONSE_CODE_MAPPING.put(ResponseCode.SYSTEM_BUSY, Code.TOO_MANY_REQUESTS);
        RESPONSE_CODE_MAPPING.put(ResponseCode.REQUEST_CODE_NOT_SUPPORTED, Code.NOT_IMPLEMENTED);
        RESPONSE_CODE_MAPPING.put(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST, Code.CONSUMER_GROUP_NOT_FOUND);
        RESPONSE_CODE_MAPPING.put(ResponseCode.LMQ_QUOTA_EXCEEDED, Code.LITE_TOPIC_QUOTA_EXCEEDED);
        RESPONSE_CODE_MAPPING.put(ResponseCode.LITE_SUBSCRIPTION_QUOTA_EXCEEDED, Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED);
        RESPONSE_CODE_MAPPING.put(ClientErrorCode.ACCESS_BROKER_TIMEOUT, Code.PROXY_TIMEOUT);
    }

    public static ResponseBuilder getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_CREATE_LOCK) {
                if (instance == null) {
                    instance = new ResponseBuilder();
                }
            }
        }
        return instance;
    }

    public Status buildStatus(Throwable t) {
        t = ExceptionUtils.getRealException(t);

        if (t instanceof ProxyException) {
            t = new GrpcProxyException((ProxyException) t);
        }
        if (t instanceof GrpcProxyException) {
            GrpcProxyException grpcProxyException = (GrpcProxyException) t;
            return ensureFailureStatus(
                buildStatus(grpcProxyException.getCode(), grpcProxyException.getMessage()), t);
        }
        if (TopicRouteHelper.isTopicNotExistError(t)) {
            return ensureFailureStatus(buildStatus(Code.TOPIC_NOT_FOUND, t.getMessage()), t);
        }
        if (t instanceof MQBrokerException) {
            MQBrokerException mqBrokerException = (MQBrokerException) t;
            return ensureFailureStatus(
                buildStatus(buildCode(mqBrokerException.getResponseCode()), mqBrokerException.getErrorMessage()), t);
        }
        if (t instanceof MQClientException) {
            MQClientException mqClientException = (MQClientException) t;
            return ensureFailureStatus(
                buildStatus(buildCode(mqClientException.getResponseCode()), mqClientException.getErrorMessage()), t);
        }
        if (t instanceof RemotingTimeoutException) {
            return ensureFailureStatus(buildStatus(Code.PROXY_TIMEOUT, t.getMessage()), t);
        }
        if (t instanceof AuthenticationException || t instanceof AuthorizationException) {
            return ensureFailureStatus(buildStatus(Code.UNAUTHORIZED, t.getMessage()), t);
        }

        log.error("internal server error", t);
        return ensureFailureStatus(
            buildStatus(Code.INTERNAL_SERVER_ERROR, ExceptionUtils.getErrorDetailMessage(t)), t);
    }

    private Status ensureFailureStatus(Status status, Throwable source) {
        if (status.getCode() != Code.OK) {
            return status;
        }
        log.error("an exceptional completion was mapped to Code.OK; overriding it with INTERNAL_SERVER_ERROR", source);
        return buildStatus(Code.INTERNAL_SERVER_ERROR, ExceptionUtils.getErrorDetailMessage(source));
    }

    public Status buildStatus(Code code, String message) {
        return Status.newBuilder()
            .setCode(code)
            .setMessage(message != null ? message : code.name())
            .build();
    }

    public Status buildStatus(int remotingResponseCode, String remark) {
        String message = remark;
        if (message == null) {
            message = String.valueOf(remotingResponseCode);
        }
        return Status.newBuilder()
            .setCode(buildCode(remotingResponseCode))
            .setMessage(message)
            .build();
    }

    public Code buildCode(int remotingResponseCode) {
        return RESPONSE_CODE_MAPPING.getOrDefault(remotingResponseCode, Code.INTERNAL_SERVER_ERROR);
    }
}
