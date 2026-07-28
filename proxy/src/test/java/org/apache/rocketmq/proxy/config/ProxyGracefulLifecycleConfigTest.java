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

package org.apache.rocketmq.proxy.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

public class ProxyGracefulLifecycleConfigTest {

    private ProxyConfig strictConfig() {
        ProxyConfig config = new ProxyConfig();
        config.setProxyMode("cluster");
        config.setEnableProxyAdminServer(true);
        config.setEnableProxyGracefulLifecycle(true);
        config.setProxyGrpcConnectionLeaseEnabled(true);
        return config;
    }

    @Test
    @DisplayName("graceful lifecycle is disabled by default for backward compatibility")
    public void defaultsFeatureOff() {
        ProxyConfig config = new ProxyConfig();
        assertThat(config.isEnableProxyGracefulLifecycle()).isFalse();
        assertThat(config.isEnableProxyAdminServer()).isFalse();
        assertThat(config.getProxyAdminPort()).isEqualTo(8082);
        assertThat(config.getProxyConnectionLeaseSeconds()).isEqualTo(300);
        assertThat(config.getProxyConnectionLeaseGraceSeconds()).isEqualTo(30);
        assertThat(config.getProxyRemotingLeaseJitterRatio()).isEqualTo(0.10);
        assertThat(config.getProxyPreStopWaitSeconds()).isEqualTo(480);
        assertThat(config.getProxyJvmShutdownTimeoutSeconds()).isEqualTo(30);
    }

    @Test
    @DisplayName("feature-off config passes validation regardless of other values")
    public void featureOffSkipsValidation() {
        ProxyConfig config = new ProxyConfig();
        config.setProxyAdminPort(-1); // would be illegal if validated
        // no throw
        config.validateGracefulLifecycle();
    }

    @Test
    @DisplayName("a well-formed strict config validates without error")
    public void strictConfigValid() {
        strictConfig().validateGracefulLifecycle();
    }

    @Test
    @DisplayName("lifecycle enabled requires admin server enabled")
    public void lifecycleRequiresAdmin() {
        ProxyConfig config = strictConfig();
        config.setEnableProxyAdminServer(false);
        assertThatThrownBy(config::validateGracefulLifecycle)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("enableProxyAdminServer");
    }

    @Test
    @DisplayName("lifecycle is only allowed in cluster mode")
    public void lifecycleRequiresClusterMode() {
        ProxyConfig config = strictConfig();
        config.setProxyMode("local");
        assertThatThrownBy(config::validateGracefulLifecycle)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cluster");
    }

    @Test
    @DisplayName("admin port must be in the valid non-privileged range")
    public void adminPortRange() {
        ProxyConfig config = strictConfig();
        config.setProxyAdminPort(80);
        assertThatThrownBy(config::validateGracefulLifecycle)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyAdminPort");
    }

    @Test
    @DisplayName("admin port must not collide with the gRPC business port")
    public void adminPortNoCollision() {
        ProxyConfig config = strictConfig();
        config.setProxyAdminPort(config.getGrpcServerPort());
        assertThatThrownBy(config::validateGracefulLifecycle)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("conflict");
    }

    @Test
    @DisplayName("jitter ratio must fall within [0, 0.5]")
    public void jitterRatioRange() {
        ProxyConfig config = strictConfig();
        config.setProxyRemotingLeaseJitterRatio(0.9);
        assertThatThrownBy(config::validateGracefulLifecycle)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("JitterRatio");
    }

    @Test
    @DisplayName("hard deadline derived from budgets must not exceed the PreStop wait")
    public void hardDeadlineWithinPreStop() {
        ProxyConfig config = strictConfig();
        // lbDetach(60)+maxLease(330)+grace(30)+sendDrain(30) = 450 > preStop 100
        config.setProxyPreStopWaitSeconds(100);
        assertThatThrownBy(config::validateGracefulLifecycle)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("PreStop");
    }

    @Test
    @DisplayName("all field errors are aggregated into a single validation exception")
    public void aggregatesAllErrors() {
        ProxyConfig config = strictConfig();
        config.setProxyAdminPort(80);
        config.setProxyRemotingLeaseJitterRatio(0.9);
        config.setProxyConnectionLeaseSeconds(1); // below min
        Throwable t = catchThrowable(config::validateGracefulLifecycle);
        assertThat(t).isInstanceOf(IllegalArgumentException.class);
        assertThat(t.getMessage()).contains("proxyAdminPort");
        assertThat(t.getMessage()).contains("JitterRatio");
        assertThat(t.getMessage()).contains("proxyConnectionLeaseSeconds");
    }

    @Test
    @DisplayName("send-drain accounting is disabled by default")
    public void sendDrainDefaultsOff() {
        assertThat(new ProxyConfig().isEnableProxySendDrain()).isFalse();
    }

    @Test
    @DisplayName("send-drain may be enabled on top of a well-formed strict config")
    public void sendDrainAllowedWithLifecycle() {
        ProxyConfig config = strictConfig();
        config.setEnableProxySendDrain(true);
        config.validateGracefulLifecycle();
    }

    @Test
    @DisplayName("send-drain cannot be enabled while the parent lifecycle flag is off")
    public void sendDrainRequiresLifecycle() {
        ProxyConfig config = new ProxyConfig();
        config.setEnableProxyGracefulLifecycle(false);
        config.setEnableProxySendDrain(true);
        assertThatThrownBy(config::validateGracefulLifecycle)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("enableProxySendDrain");
    }
}
