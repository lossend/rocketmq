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
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TopicClientInfoIndexTest {

    private static final String TOPIC = "T";
    private static final String LOGICAL_GROUP = "G";

    private TopicClientInfoIndex index;

    @Before
    public void setUp() {
        index = new TopicClientInfoIndex();
    }

    private ClientChannelInfo clientChannelInfo(String clientId) {
        return new ClientChannelInfo(null, clientId, LanguageCode.JAVA, 0);
    }

    private Set<String> topics(String... topics) {
        Set<String> set = new HashSet<>();
        Collections.addAll(set, topics);
        return set;
    }

    @Test
    public void should_track_gray_label_when_client_register_with_labeled_group() {
        index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G%gray1",
            clientChannelInfo("c1"), topics(TOPIC));

        assertThat(index.getActiveIsolatedLabels(TOPIC, LOGICAL_GROUP)).containsExactly("gray1");
    }

    @Test
    public void should_not_track_any_label_when_standard_client_registers() {
        index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G",
            clientChannelInfo("c1"), topics(TOPIC));

        assertThat(index.getActiveIsolatedLabels(TOPIC, LOGICAL_GROUP)).isEmpty();
    }

    @Test
    public void should_add_and_remove_gray_labels_across_register_and_unregister() {
        index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G%gray1",
            clientChannelInfo("c1"), topics(TOPIC));
        index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G%gray2",
            clientChannelInfo("c2"), topics(TOPIC));

        assertThat(index.getActiveIsolatedLabels(TOPIC, LOGICAL_GROUP))
            .containsExactlyInAnyOrder("gray1", "gray2");

        index.handle(ConsumerGroupEvent.CLIENT_UNREGISTER, "G%gray1",
            clientChannelInfo("c1"), topics(TOPIC));

        assertThat(index.getActiveIsolatedLabels(TOPIC, LOGICAL_GROUP)).containsExactly("gray2");

        index.handle(ConsumerGroupEvent.CLIENT_UNREGISTER, "G%gray2",
            clientChannelInfo("c2"), topics(TOPIC));

        assertThat(index.getActiveIsolatedLabels(TOPIC, LOGICAL_GROUP)).isEmpty();
    }

    @Test
    public void should_return_empty_set_for_unknown_topic_or_group() {
        assertThat(index.getActiveIsolatedLabels("unknown-topic", LOGICAL_GROUP)).isNotNull().isEmpty();
        assertThat(index.getActiveIsolatedLabels(TOPIC, "unknown-group")).isNotNull().isEmpty();

        index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G%gray1",
            clientChannelInfo("c1"), topics(TOPIC));

        assertThat(index.getActiveIsolatedLabels(TOPIC, "other-group")).isNotNull().isEmpty();
    }

    @Test
    public void should_ignore_non_client_events_without_throwing() {
        assertThatCode(() -> {
            index.handle(ConsumerGroupEvent.CHANGE, "G%gray1", (Object[]) null);
            index.handle(ConsumerGroupEvent.REGISTER, "G%gray1", clientChannelInfo("c1"), topics(TOPIC));
            index.handle(ConsumerGroupEvent.UNREGISTER, "G%gray1", new Object[] {"garbage"});
        }).doesNotThrowAnyException();

        assertThat(index.getActiveIsolatedLabels(TOPIC, LOGICAL_GROUP)).isEmpty();
    }

    @Test
    public void should_not_throw_on_malformed_args_for_client_events() {
        assertThatCode(() -> {
            index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G%gray1", (Object[]) null);
            index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G%gray1");
            index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G%gray1", clientChannelInfo("c1"));
            index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G%gray1", "not-a-channel", "not-a-set");
            index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G%gray1", clientChannelInfo("c1"), "not-a-set");
            index.handle(ConsumerGroupEvent.CLIENT_UNREGISTER, "G%gray1", (Object[]) null);
            index.handle(ConsumerGroupEvent.CLIENT_UNREGISTER, "G%gray1", new Object[] {null, null});
        }).doesNotThrowAnyException();

        assertThat(index.getActiveIsolatedLabels(TOPIC, LOGICAL_GROUP)).isEmpty();
    }
}
