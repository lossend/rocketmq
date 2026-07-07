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

/**
 * Constants and utilities for traffic label routing.
 * A label of {@code null}, empty, or {@value #STANDARD} is treated as the standard (non-gray)
 * traffic lane; any other non-empty value identifies a gray lane.
 */
public final class TrafficLabel {

    /**
     * Message property key used to carry the traffic label.
     * Matches the agent SDK's {@code Constants.SERVICE_TAG} so producers and consumers
     * use the same key without translation.
     */
    public static final String PROPERTY_KEY = "__SERVICE_TAG__";

    /** Sentinel value representing the standard (non-gray) traffic lane. */
    public static final String STANDARD = "default";

    /** Separator inserted between the origin consumer group and a gray label. */
    public static final String GROUP_SEPARATOR = "%";

    private TrafficLabel() {}

    /**
     * Returns {@code true} when {@code label} designates a gray lane,
     * i.e. it is non-null, non-blank, and not equal to {@value #STANDARD}.
     *
     * @param label the traffic label to test, may be {@code null}
     * @return {@code true} for gray labels, {@code false} otherwise
     */
    public static boolean isGray(String label) {
        return label != null && !label.trim().isEmpty() && !STANDARD.equals(label);
    }

    /**
     * Returns the effective consumer-group name for the given label.
     * Gray labels produce a virtual group of the form {@code originGroup%label};
     * standard labels return {@code originGroup} unchanged.
     *
     * @param originGroup the original consumer group name
     * @param label       the traffic label, may be {@code null}
     * @return the effective consumer group name
     */
    public static String effectiveGroup(String originGroup, String label) {
        if (!isGray(label)) {
            return originGroup;
        }
        return originGroup + GROUP_SEPARATOR + label;
    }

    /**
     * Extracts the logical (origin) group from an effective group name.
     * Given a virtual group of the form {@code G%gray1} returns {@code G};
     * given a plain group with no separator returns it unchanged.
     *
     * @param effectiveGroup the effective consumer-group name, may be {@code null}
     * @return the logical group name, or {@code null} when {@code effectiveGroup} is {@code null}
     */
    public static String parseLogicalGroup(String effectiveGroup) {
        if (effectiveGroup == null) {
            return null;
        }
        int idx = effectiveGroup.indexOf(GROUP_SEPARATOR);
        if (idx < 0) {
            return effectiveGroup;
        }
        return effectiveGroup.substring(0, idx);
    }

    /**
     * Extracts the traffic label from an effective group name.
     * Given a virtual group of the form {@code G%gray1} returns {@code gray1};
     * given a plain group with no separator returns {@code null}.
     *
     * @param effectiveGroup the effective consumer-group name, may be {@code null}
     * @return the traffic label, or {@code null} when no label is present
     */
    public static String parseLabel(String effectiveGroup) {
        if (effectiveGroup == null) {
            return null;
        }
        int idx = effectiveGroup.indexOf(GROUP_SEPARATOR);
        if (idx < 0) {
            return null;
        }
        return effectiveGroup.substring(idx + GROUP_SEPARATOR.length());
    }
}
