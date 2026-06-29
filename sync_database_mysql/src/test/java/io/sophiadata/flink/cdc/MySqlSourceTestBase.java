/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sophiadata.flink.cdc;

import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import io.sophiadata.flink.utils.MySqlContainer;
import io.sophiadata.flink.utils.MySqlVersion;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** (@sophiadata) (@date 2023/6/1 14:24). */
public abstract class MySqlSourceTestBase extends TestLogger {

    protected static final Logger LOG = LoggerFactory.getLogger(MySqlSourceTestBase.class);

    protected static final int DEFAULT_PARALLELISM = 4;
    protected static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer();

    private static MySqlContainer createMySqlContainer() {
        // Use V8_LATEST when -Dtestcontainers.mysql.image=mysql:latest is set, so the suite
        // can run against a locally cached image without re-pulling from Docker Hub.
        String imageOverride = System.getProperty("testcontainers.mysql.image", "");
        MySqlVersion version =
                "mysql:latest".equals(imageOverride) ? MySqlVersion.V8_LATEST : MySqlVersion.V8_0;
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withDatabaseName("flink-test")
                .withUsername("flinkuser")
                .withPassword("flinkpw");
    }

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @BeforeClass
    public static void startContainers() {
        if (!Boolean.getBoolean("runIntegrationTests")) {
            // Skip the Docker pull when CDC tests are not enabled.
            org.junit.Assume.assumeTrue(
                    "set -DrunIntegrationTests=true to start the Testcontainers MySQL instance",
                    false);
            return;
        }
        LOG.info("Starting MySQL container...");
        try {
            MYSQL_CONTAINER.start();
        } catch (Throwable t) {
            org.junit.Assume.assumeNoException(
                    "Testcontainers MySQL failed to start (Docker available? image pulled?): "
                            + t.getMessage(),
                    t);
        }
        LOG.info(
                "MySQL container started at {}:{}",
                MYSQL_CONTAINER.getHost(),
                MYSQL_CONTAINER.getDatabasePort());
    }

    @AfterClass
    public static void stopContainers() {
        if (MYSQL_CONTAINER != null) {
            try {
                MYSQL_CONTAINER.stop();
            } catch (Throwable ignored) {
                // best-effort cleanup
            }
        }
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }
}
