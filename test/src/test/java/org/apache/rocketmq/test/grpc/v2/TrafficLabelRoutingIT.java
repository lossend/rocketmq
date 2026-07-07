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
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.Resource;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.common.utils.NetworkUtil;
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
 *   <li><strong>Gray consumer</strong> — sends gRPC header {@code __SERVICE_TAG__: gray1}
 *       (grpc-java normalizes the wire header to {@code __service_tag__}).
 *       Receives only messages whose {@code __SERVICE_TAG__} property equals {@code gray1}.</li>
 *   <li><strong>Standard consumer</strong> — sends no traffic-label header.
 *       While any gray consumer is online for the same (topic, group), receives only messages
 *       whose {@code __SERVICE_TAG__} is absent.</li>
 * </ol>
 *
 * <p>Prerequisites satisfied by the test framework:
 * <ul>
 *   <li>In-process broker started with {@code enablePropertyFilter=true}.</li>
 *   <li>{@code ContextInitPipeline} copies the {@code __SERVICE_TAG__} gRPC header into
 *       {@code ProxyContext}.</li>
 *   <li>{@code DefaultGrpcMessagingActivity.init()} registers {@code TopicClientInfoIndex} as a
 *       {@code ConsumerIdsChangeListener} before the processor starts.</li>
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
        // Pass 0 so each test gets a fresh ephemeral port; setUpServer stores the actual port back.
        setUpServer(grpcMessagingApplication, 0, true);
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
    }

    @After
    public void clean() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
        messagingProcessor.shutdown();
        grpcMessagingApplication.shutdown();
        shutdown();
    }

    // -------------------------------------------------------------------------
    // E2E-C: Gray consumer regression — receives only gray-labeled messages
    // -------------------------------------------------------------------------

    /**
     * Gray consumer receives only messages carrying {@code __SERVICE_TAG__=gray1}.
     */
    @Test
    public void gray_consumer_receives_only_gray_labeled_messages() throws Exception {
        String topic = initTopic();
        String group = MQRandomUtils.getRandomConsumerGroup();
        initConsumerGroup(group);

        LabeledStubs grayStubs = createLabeledStubs(GRAY_LABEL);

        // Init consumer offset (drain any old messages).
        sendClientSettings(grayStubs.async, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(grayStubs.blocking, topic, group, 2);

        sendClientSettings(stub, buildProducerClientSettings(topic)).get();

        String grayMsgId = createUniqID();
        assertSendMessage(
            blockingStub.sendMessage(buildSendMessageRequestWithLabel(topic, grayMsgId, GRAY_LABEL)),
            grayMsgId);

        String stdMsgId = createUniqID();
        assertSendMessage(blockingStub.sendMessage(buildSendMessageRequest(topic, stdMsgId)), stdMsgId);

        sendClientSettings(grayStubs.async, buildSimpleConsumerClientSettings(group)).get();
        await().atMost(java.time.Duration.ofSeconds(15)).untilAsserted(() -> {
            List<Message> received = getMessageFromReceiveMessageResponse(
                receiveMessage(grayStubs.blocking, topic, group, 5));
            assertThat(received).isNotEmpty();
            for (Message msg : received) {
                assertThat(msg.getUserPropertiesMap().get(TrafficLabel.PROPERTY_KEY))
                    .as("Gray consumer must only receive messages with label " + GRAY_LABEL)
                    .isEqualTo(GRAY_LABEL);
            }
        });
    }

    // -------------------------------------------------------------------------
    // E2E-A: Gray consumer online → standard consumer excludes gray messages
    // -------------------------------------------------------------------------

    /**
     * While a gray consumer is online, the standard consumer must not receive gray-labeled messages.
     *
     * <p>The gray consumer registration rewrites the group to {@code G%gray1} at the broker, which
     * causes the broker to emit {@code CLIENT_REGISTER(G%gray1, [topic])}. The
     * {@code TopicClientInfoIndex} listener picks this up so that the standard consumer's
     * {@code ReceiveMessage} call gets a dynamic exclusion filter.
     */
    @Test
    public void standard_consumer_does_not_receive_gray_labeled_messages_while_gray_is_online()
        throws Exception {
        String topic = initTopic();
        String group = MQRandomUtils.getRandomConsumerGroup();
        initConsumerGroup(group);

        LabeledStubs grayStubs = createLabeledStubs(GRAY_LABEL);
        MessagingServiceGrpc.MessagingServiceBlockingStub standardStub = createStandardBlockingStub();

        // Register gray consumer so CLIENT_REGISTER fires and the index is populated.
        sendClientSettings(grayStubs.async, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(grayStubs.blocking, topic, group, 2);

        // Init standard consumer offset.
        sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(standardStub, topic, group, 2);

        sendClientSettings(stub, buildProducerClientSettings(topic)).get();

        String stdMsgId = createUniqID();
        assertSendMessage(blockingStub.sendMessage(buildSendMessageRequest(topic, stdMsgId)), stdMsgId);

        String grayMsgId = createUniqID();
        assertSendMessage(
            blockingStub.sendMessage(buildSendMessageRequestWithLabel(topic, grayMsgId, GRAY_LABEL)),
            grayMsgId);

        sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        await().atMost(java.time.Duration.ofSeconds(15)).untilAsserted(() -> {
            List<Message> received = getMessageFromReceiveMessageResponse(
                receiveMessage(standardStub, topic, group, 5));
            assertThat(received).isNotEmpty();
            for (Message msg : received) {
                assertThat(msg.getUserPropertiesMap().get(TrafficLabel.PROPERTY_KEY))
                    .as("Standard consumer must not receive gray-labeled messages")
                    .isNotEqualTo(GRAY_LABEL);
            }
        });
    }

    // -------------------------------------------------------------------------
    // E2E-B: Gray consumer offline → exclusion removed, standard can consume
    // -------------------------------------------------------------------------

    /**
     * After the gray consumer terminates, the index clears and the standard consumer can receive
     * gray-labeled messages (no exclusion filter is applied).
     *
     * <p>{@code NotifyClientTermination} triggers {@code unRegisterConsumer} synchronously, which
     * causes the broker to emit {@code CLIENT_UNREGISTER(G%gray1, ...)} before the RPC returns.
     * The index is updated in the same event callback, so it is safe to assert immediately after.
     */
    @Test
    public void standard_consumer_receives_gray_messages_after_gray_consumer_terminates()
        throws Exception {
        String topic = initTopic();
        String group = MQRandomUtils.getRandomConsumerGroup();
        initConsumerGroup(group);

        LabeledStubs grayStubs = createLabeledStubs(GRAY_LABEL);
        MessagingServiceGrpc.MessagingServiceBlockingStub standardStub = createStandardBlockingStub();

        // Register gray consumer and init offsets.
        sendClientSettings(grayStubs.async, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(grayStubs.blocking, topic, group, 2);

        sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(standardStub, topic, group, 2);

        // Gray consumer goes offline — broker fires CLIENT_UNREGISTER, index clears.
        grayStubs.blocking.notifyClientTermination(
            NotifyClientTerminationRequest.newBuilder()
                .setGroup(Resource.newBuilder().setName(group).build())
                .build());

        // Produce a gray-labeled message AFTER the gray consumer is gone.
        sendClientSettings(stub, buildProducerClientSettings(topic)).get();
        String grayMsgId = createUniqID();
        assertSendMessage(
            blockingStub.sendMessage(buildSendMessageRequestWithLabel(topic, grayMsgId, GRAY_LABEL)),
            grayMsgId);

        // Standard consumer should now receive it (no exclusion filter active).
        sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        await().atMost(java.time.Duration.ofSeconds(15)).untilAsserted(() -> {
            List<Message> received = getMessageFromReceiveMessageResponse(
                receiveMessage(standardStub, topic, group, 5));
            assertThat(received).isNotEmpty();
            boolean foundGray = received.stream().anyMatch(
                m -> GRAY_LABEL.equals(m.getUserPropertiesMap().get(TrafficLabel.PROPERTY_KEY)));
            assertThat(foundGray)
                .as("Standard consumer should receive gray-labeled messages after gray consumer terminates")
                .isTrue();
        });
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Paired blocking + async stubs that share the same client-id and traffic-label header. */
    private static class LabeledStubs {
        final MessagingServiceGrpc.MessagingServiceBlockingStub blocking;
        final MessagingServiceGrpc.MessagingServiceStub async;

        LabeledStubs(MessagingServiceGrpc.MessagingServiceBlockingStub blocking,
            MessagingServiceGrpc.MessagingServiceStub async) {
            this.blocking = blocking;
            this.async = async;
        }
    }

    /**
     * Creates paired stubs sharing a stable client-id and the given traffic label.
     * Use {@link #createStandardBlockingStub()} for the standard (no-label) lane.
     */
    private LabeledStubs createLabeledStubs(String label) throws Exception {
        String clientId = "client-" + label + "-" + UUID.randomUUID();
        Metadata meta = new Metadata();
        meta.merge(header);
        meta.put(GrpcConstants.CLIENT_ID, clientId);
        meta.put(GrpcConstants.TRAFFIC_LABEL, label);

        Channel ch = createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort());
        return new LabeledStubs(
            MessagingServiceGrpc.newBlockingStub(ch)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta)),
            MessagingServiceGrpc.newStub(ch)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta)));
    }

    /** Creates a blocking stub without a traffic-label header (standard lane). */
    private MessagingServiceGrpc.MessagingServiceBlockingStub createStandardBlockingStub()
        throws Exception {
        Metadata meta = new Metadata();
        meta.merge(header);
        meta.put(GrpcConstants.CLIENT_ID, "client-standard-" + UUID.randomUUID());

        return MessagingServiceGrpc.newBlockingStub(
                createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort()))
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
    }

    private apache.rocketmq.v2.SendMessageRequest buildSendMessageRequestWithLabel(
        String topic, String messageId, String label) {
        return apache.rocketmq.v2.SendMessageRequest.newBuilder()
            .addMessages(apache.rocketmq.v2.Message.newBuilder()
                .setTopic(apache.rocketmq.v2.Resource.newBuilder().setName(topic).build())
                .setSystemProperties(apache.rocketmq.v2.SystemProperties.newBuilder()
                    .setMessageId(messageId)
                    .setQueueId(0)
                    .setMessageType(apache.rocketmq.v2.MessageType.NORMAL)
                    .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                    .setBornHost(StringUtils.defaultString(
                        NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                    .build())
                .putUserProperties(TrafficLabel.PROPERTY_KEY, label)
                .setBody(ByteString.copyFromUtf8("traffic-label-test"))
                .build())
            .build();
    }
}
