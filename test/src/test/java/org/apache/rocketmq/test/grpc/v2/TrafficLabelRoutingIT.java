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

package org.apache.rocketmq.test.grpc.v2;

import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessagingServiceGrpc;
import apache.rocketmq.v2.SendMessageResponse;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.List;
import java.util.UUID;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.grpc.v2.consumer.TrafficLabel;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.common.message.MessageClientIDSetter.createUniqID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * E2E integration test for traffic-label isolation routing via gRPC.
 *
 * <p>Two consumer personas are tested:
 * <ol>
 *   <li><strong>Gray consumer</strong> — sends gRPC header {@code __rmq_traffic_label: gray1}.
 *       It must receive only messages whose {@code __RMQ_TRAFFIC_LABEL} property equals {@code gray1}.</li>
 *   <li><strong>Standard consumer</strong> — sends no traffic-label header.
 *       It must receive only messages whose {@code __RMQ_TRAFFIC_LABEL} property is absent or equals
 *       {@value TrafficLabel#STANDARD}.</li>
 * </ol>
 *
 * <p>Prerequisites already satisfied by the test framework:
 * <ul>
 *   <li>The in-process broker is started with {@code enablePropertyFilter=true} (see
 *       {@code IntegrationTestBase.createAndStartBroker}).</li>
 *   <li>{@code ContextInitPipeline} copies the {@code __rmq_traffic_label} gRPC header into
 *       {@code ProxyContext} so the router can read it.</li>
 * </ul>
 */
public class TrafficLabelRoutingIT extends GrpcBaseIT {

    private static final String GRAY_LABEL = "gray1";

    private MessagingProcessor messagingProcessor;
    private GrpcMessagingApplication grpcMessagingApplication;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        messagingProcessor = DefaultMessagingProcessor.createForLocalMode(brokerController1);
        messagingProcessor.start();
        grpcMessagingApplication = GrpcMessagingApplication.create(messagingProcessor);
        grpcMessagingApplication.start();
        setUpServer(grpcMessagingApplication, ConfigurationManager.getProxyConfig().getGrpcServerPort(), true);

        // Enable traffic-label routing so the router is active during these tests.
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
    }

    @After
    public void clean() throws Exception {
        // Restore default to avoid leaking state into other test classes.
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
        messagingProcessor.shutdown();
        grpcMessagingApplication.shutdown();
        shutdown();
    }

    // -------------------------------------------------------------------------
    // Helper: build a blocking stub that attaches the traffic-label header
    // -------------------------------------------------------------------------

    /**
     * Creates a blocking stub that attaches the given traffic label as a gRPC metadata header.
     * The header name matches {@link GrpcConstants#TRAFFIC_LABEL} (i.e. {@code __rmq_traffic_label}).
     *
     * @param label the traffic label to inject, e.g. {@code "gray1"}
     * @return a blocking stub scoped to that label
     */
    private MessagingServiceGrpc.MessagingServiceBlockingStub createLabeledBlockingStub(String label)
        throws Exception {
        Metadata labeledHeader = new Metadata();
        // Copy all entries from the shared header (client-id, language) then add the label.
        labeledHeader.merge(header);
        // Each labeled consumer needs its own unique client-id to avoid conflicts.
        labeledHeader.put(GrpcConstants.CLIENT_ID, "client-" + label + "-" + UUID.randomUUID());
        labeledHeader.put(GrpcConstants.TRAFFIC_LABEL, label);

        MessagingServiceGrpc.MessagingServiceBlockingStub stub =
            MessagingServiceGrpc.newBlockingStub(createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort()));
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(labeledHeader));
    }

    /**
     * Creates a blocking stub without a traffic-label header (standard lane).
     *
     * @return a standard-lane blocking stub
     */
    private MessagingServiceGrpc.MessagingServiceBlockingStub createStandardBlockingStub()
        throws Exception {
        Metadata standardHeader = new Metadata();
        standardHeader.merge(header);
        standardHeader.put(GrpcConstants.CLIENT_ID, "client-standard-" + UUID.randomUUID());
        // Intentionally: no TRAFFIC_LABEL put here.

        MessagingServiceGrpc.MessagingServiceBlockingStub stub =
            MessagingServiceGrpc.newBlockingStub(createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort()));
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(standardHeader));
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * Gray consumer (header {@code __rmq_traffic_label: gray1}) receives only gray-labeled messages.
     *
     * <p>Flow:
     * <ol>
     *   <li>Send one message with {@code __RMQ_TRAFFIC_LABEL=gray1}.</li>
     *   <li>Send one message with no label (standard lane).</li>
     *   <li>Gray-labeled consumer calls {@code ReceiveMessage} — must see exactly the gray message.</li>
     * </ol>
     */
    @Test
    public void gray_consumer_receives_only_gray_labeled_messages() throws Exception {
        String topic = initTopic();
        String group = MQRandomUtils.getRandomConsumerGroup();
        initConsumerGroup(group);

        MessagingServiceGrpc.MessagingServiceBlockingStub producerStub = blockingStub;
        MessagingServiceGrpc.MessagingServiceBlockingStub grayConsumerStub = createLabeledBlockingStub(GRAY_LABEL);

        // Initialize consumer offset so the consumer does not replay stale messages.
        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(grayConsumerStub, topic, group, 2);

        // Producer settings.
        this.sendClientSettings(stub, buildProducerClientSettings(topic)).get();

        // Send a gray-labeled message.
        String grayMsgId = createUniqID();
        SendMessageResponse grayResp = producerStub.sendMessage(
            buildSendMessageRequestWithLabel(topic, grayMsgId, GRAY_LABEL));
        assertSendMessage(grayResp, grayMsgId);

        // Send a standard (unlabeled) message.
        String stdMsgId = createUniqID();
        SendMessageResponse stdResp = producerStub.sendMessage(buildSendMessageRequest(topic, stdMsgId));
        assertSendMessage(stdResp, stdMsgId);

        // Gray consumer must receive the gray message.
        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        await().atMost(java.time.Duration.ofSeconds(15)).untilAsserted(() -> {
            List<Message> received = getMessageFromReceiveMessageResponse(
                receiveMessage(grayConsumerStub, topic, group, 5));
            assertThat(received).isNotEmpty();
            // Every message the gray consumer receives must carry the gray label.
            for (Message msg : received) {
                String labelProp = msg.getUserPropertiesMap().get(TrafficLabel.PROPERTY_KEY);
                assertThat(labelProp)
                    .as("Gray consumer must only receive messages with label " + GRAY_LABEL)
                    .isEqualTo(GRAY_LABEL);
            }
        });
    }

    /**
     * Standard consumer (no traffic-label header) receives only unlabeled / standard messages.
     *
     * <p>Flow:
     * <ol>
     *   <li>Send one message with no label.</li>
     *   <li>Send one message with {@code __RMQ_TRAFFIC_LABEL=gray1}.</li>
     *   <li>Standard consumer calls {@code ReceiveMessage} — must see the unlabeled message only.</li>
     * </ol>
     */
    @Test
    public void standard_consumer_does_not_receive_gray_labeled_messages() throws Exception {
        String topic = initTopic();
        String group = MQRandomUtils.getRandomConsumerGroup();
        initConsumerGroup(group);

        MessagingServiceGrpc.MessagingServiceBlockingStub producerStub = blockingStub;
        MessagingServiceGrpc.MessagingServiceBlockingStub standardConsumerStub = createStandardBlockingStub();

        // Initialize consumer offset.
        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(standardConsumerStub, topic, group, 2);

        // Producer settings.
        this.sendClientSettings(stub, buildProducerClientSettings(topic)).get();

        // Send a standard (unlabeled) message.
        String stdMsgId = createUniqID();
        SendMessageResponse stdResp = producerStub.sendMessage(buildSendMessageRequest(topic, stdMsgId));
        assertSendMessage(stdResp, stdMsgId);

        // Send a gray-labeled message.
        String grayMsgId = createUniqID();
        SendMessageResponse grayResp = producerStub.sendMessage(
            buildSendMessageRequestWithLabel(topic, grayMsgId, GRAY_LABEL));
        assertSendMessage(grayResp, grayMsgId);

        // Standard consumer must receive only the standard message.
        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        await().atMost(java.time.Duration.ofSeconds(15)).untilAsserted(() -> {
            List<Message> received = getMessageFromReceiveMessageResponse(
                receiveMessage(standardConsumerStub, topic, group, 5));
            assertThat(received).isNotEmpty();
            // No message in standard lane should carry a gray label.
            for (Message msg : received) {
                String labelProp = msg.getUserPropertiesMap().get(TrafficLabel.PROPERTY_KEY);
                assertThat(labelProp)
                    .as("Standard consumer must not receive messages with gray label " + GRAY_LABEL)
                    .isNotEqualTo(GRAY_LABEL);
            }
        });
    }

    // -------------------------------------------------------------------------
    // Builders
    // -------------------------------------------------------------------------

    /**
     * Builds a {@link apache.rocketmq.v2.SendMessageRequest} with a {@code __RMQ_TRAFFIC_LABEL}
     * user property set to {@code label}.
     *
     * @param topic     topic name
     * @param messageId client-assigned message id
     * @param label     traffic label value to embed as a user property
     * @return ready-to-send request
     */
    private apache.rocketmq.v2.SendMessageRequest buildSendMessageRequestWithLabel(
        String topic, String messageId, String label) {
        return apache.rocketmq.v2.SendMessageRequest.newBuilder()
            .addMessages(apache.rocketmq.v2.Message.newBuilder()
                .setTopic(apache.rocketmq.v2.Resource.newBuilder().setName(topic).build())
                .setSystemProperties(apache.rocketmq.v2.SystemProperties.newBuilder()
                    .setMessageId(messageId)
                    .setQueueId(0)
                    .setMessageType(apache.rocketmq.v2.MessageType.NORMAL)
                    .setBornTimestamp(com.google.protobuf.util.Timestamps.fromMillis(System.currentTimeMillis()))
                    .setBornHost(org.apache.commons.lang3.StringUtils.defaultString(
                        org.apache.rocketmq.common.utils.NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                    .build())
                .putUserProperties(TrafficLabel.PROPERTY_KEY, label)
                .setBody(com.google.protobuf.ByteString.copyFromUtf8("traffic-label-test"))
                .build())
            .build();
    }
}
