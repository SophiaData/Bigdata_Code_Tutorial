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

package io.sophiadata.flink.sync;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * End-to-end test for {@link FlinkSqlWDS} on a {@link
 * org.apache.flink.test.util.MiniClusterWithClientResource}. Requires two pre-started MySQL 8
 * instances; configure them via system properties.
 *
 * <p>Run with:
 *
 * <pre>
 *   mvn -pl sync_database_mysql -DrunIntegrationTests=true \
 *       -Dmysql.it.source.url=jdbc:mysql://host:3306/source_db?useSSL=false \
 *       -Dmysql.it.source.user=root -Dmysql.it.source.password=root \
 *       -Dmysql.it.sink.url=jdbc:mysql://host:3306/sink_db?useSSL=false \
 *       -Dmysql.it.sink.user=root -Dmysql.it.sink.password=root \
 *       -Dtest=FlinkSqlWDSTest -DfailIfNoTests=false test
 * </pre>
 */
public class FlinkSqlWDSTest {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlWDSTest.class);

    @ClassRule
    public static final org.apache.flink.test.util.MiniClusterWithClientResource MINI_CLUSTER =
            new org.apache.flink.test.util.MiniClusterWithClientResource(
                    new org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
                                    .Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private static String sourceUrl;
    private static String sourceUser;
    private static String sourcePw;
    private static String sinkUrl;
    private static String sinkUser;
    private static String sinkPw;

    @BeforeClass
    public static void loadProperties() {
        assumeTrue(
                "set -DrunIntegrationTests=true and -Dmysql.it.source.url/-Dmysql.it.sink.url to enable",
                Boolean.getBoolean("runIntegrationTests"));
        sourceUrl = System.getProperty("mysql.it.source.url");
        sourceUser = System.getProperty("mysql.it.source.user", "root");
        sourcePw = System.getProperty("mysql.it.source.password", "root");
        sinkUrl = System.getProperty("mysql.it.sink.url");
        sinkUser = System.getProperty("mysql.it.sink.user", "root");
        sinkPw = System.getProperty("mysql.it.sink.password", "root");
        assumeTrue("set -Dmysql.it.source.url=...", sourceUrl != null);
        assumeTrue("set -Dmysql.it.sink.url=...", sinkUrl != null);
    }

    @Test
    public void testWholeDatabaseSync_replicatesRows() throws Exception {
        // 1) Seed source: create table and insert 2 rows.
        try (Connection c = DriverManager.getConnection(sourceUrl, sourceUser, sourcePw);
                Statement st = c.createStatement()) {
            st.executeUpdate("DROP TABLE IF EXISTS flink_source.t_user");
            st.executeUpdate("USE flink_source");
            st.executeUpdate(
                    "CREATE TABLE t_user ("
                            + "  id BIGINT NOT NULL, "
                            + "  name VARCHAR(255), "
                            + "  age TINYINT, "
                            + "  create_time TIMESTAMP(0) NOT NULL DEFAULT '1970-01-01 09:00:00', "
                            + "  update_time TIMESTAMP(0) NOT NULL DEFAULT '1970-01-01 09:00:00', "
                            + "  PRIMARY KEY (id))");
            st.executeUpdate(
                    "INSERT INTO t_user VALUES "
                            + "(1, 'alice', 30, NOW(), NOW()),"
                            + "(2, 'bob', 25, NOW(), NOW())");
        }

        // 2) Build parameter map and run the WDS job on the mini cluster.
        Map<String, String> args = new HashMap<>();
        args.put("hostname", hostOf(sourceUrl));
        args.put("port", portOf(sourceUrl));
        args.put("username", sourceUser);
        args.put("password", sourcePw);
        args.put("databaseName", "flink_source");
        args.put("tableList", "flink_source.t_user");
        args.put("sinkUrl", sinkUrl);
        args.put("sinkUsername", sinkUser);
        args.put("sinkPassword", sinkPw);
        args.put("setParallelism", "1");
        args.put("cdcSourceName", "mysql-cdc-it");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig()
                .getConfiguration()
                .setString("pipeline.name", "wds-it-" + System.currentTimeMillis());

        new FlinkSqlWDS().handle(toArgs(args), env, tEnv);

        // 3) Wait up to 2 minutes for sink rows to land.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        int count = 0;
        while (System.nanoTime() < deadline) {
            try (Connection c = DriverManager.getConnection(sinkUrl, sinkUser, sinkPw);
                    Statement st = c.createStatement()) {
                st.executeUpdate("USE flink_sink");
                try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM sink_t_user")) {
                    if (rs.next()) {
                        count = rs.getInt(1);
                        if (count >= 2) {
                            break;
                        }
                    }
                }
            } catch (Exception ignored) {
                // table may not exist yet
            }
            Thread.sleep(2000);
        }
        assertTrue("expected at least 2 rows in sink_t_user, got " + count, count >= 2);
    }

    private static String[] toArgs(Map<String, String> m) {
        String[] out = new String[m.size() * 2];
        int i = 0;
        for (Map.Entry<String, String> e : m.entrySet()) {
            out[i++] = "--" + e.getKey();
            out[i++] = e.getValue();
        }
        return out;
    }

    private static String hostOf(String url) {
        int s = url.indexOf("://") + 3;
        int e = url.indexOf(':', s);
        if (e == -1) {
            e = url.indexOf('/', s);
        }
        if (e == -1) {
            e = url.indexOf('?', s);
        }
        return url.substring(s, e);
    }

    private static String portOf(String url) {
        int s = url.indexOf("://") + 3;
        int colon = url.indexOf(':', s);
        if (colon == -1) {
            return "3306";
        }
        int end = url.length();
        int slash = url.indexOf('/', colon);
        int qmark = url.indexOf('?', colon);
        if (slash != -1) {
            end = slash;
        }
        if (qmark != -1 && qmark < end) {
            end = qmark;
        }
        return url.substring(colon + 1, end);
    }

    @SuppressWarnings("unused")
    private void touch() {
        ParameterTool p = ParameterTool.fromArgs(new String[] {});
        LOG.info("touch {}", p);
    }
}
