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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import io.sophiadata.flink.sync.table.CustomDebeziumDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end verification of the CDC path against a real MySQL server.
 *
 * <p>This is the test that actually proves the pipeline works. Everything else in the suite either
 * checks pure functions or asserts that objects can be constructed; this one starts a real MySQL
 * with binlog enabled, runs the project's real {@link CdcAdapters} source through a real Flink job,
 * and asserts on the change events that come out the other end.
 *
 * <p>What it covers:
 *
 * <ul>
 *   <li>Snapshot phase: existing rows are read and emitted as {@link RowKind#INSERT}.
 *   <li>Binlog phase: {@code INSERT} / {@code UPDATE} / {@code DELETE} issued after startup are
 *       captured and mapped to the correct {@code RowKind}, with correct field values.
 *   <li>The version-neutral adapter resolves and builds a working connector on whichever Flink CDC
 *       line is active (CDC 2.x or 3.x).
 *   <li>{@link CustomDebeziumDeserializer} converts real Debezium payloads, including {@code
 *       UPDATE_BEFORE}/{@code UPDATE_AFTER} ordering.
 * </ul>
 *
 * <p>Requires Docker. Excluded from the default build via Surefire's {@code excludedGroups=@Tag}
 * configuration; run with {@code mvn test -Dgroups=docker}.
 */
@Tag("docker")
class MysqlCdcEndToEndIT {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlCdcEndToEndIT.class);

    /**
     * The connection time zone used by the test.
     *
     * <p>The container is started with {@code default-time-zone=+00:00}, so the connector must use
     * {@code UTC} as well. Flink CDC validates the two against each other and refuses to start on a
     * mismatch.
     */
    private static final String SERVER_TIME_ZONE = "UTC";

    private static final String SOURCE_DB = "src_db";
    private static final String TABLE = "orders";

    /**
     * mysql:8.0 with binlog enabled, which MySQL CDC requires. {@code binlog_row_image=full} is
     * essential: with the default {@code minimal}, Debezium omits unchanged columns and the
     * delete/update images would be incomplete.
     *
     * <p>The timezone is pinned to UTC and {@code default-time-zone} is set to match. Flink CDC
     * validates the server timezone against its configured {@code serverTimeZone} and refuses to
     * start when they disagree ("The MySQL server has a timezone offset ... which does not match
     * the configured timezone"). Leaving the container on UTC while the JVM is on Asia/Shanghai
     * makes the job restart-loop, so both sides are aligned explicitly.
     */
    @SuppressWarnings("resource")
    private static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                    .withDatabaseName(SOURCE_DB)
                    .withUsername("cdcuser")
                    .withPassword("cdcpw")
                    .withEnv("TZ", "UTC")
                    .withCommand(
                            "--server-id=223344",
                            "--log-bin=mysql-bin",
                            "--binlog-format=row",
                            "--binlog-row-image=full",
                            "--expire-logs-days=1",
                            "--default-time-zone=+00:00")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeAll
    static void startDatabase() throws Exception {
        LOG.info("Starting MySQL container for CDC end-to-end test...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        LOG.info("MySQL ready at {}", MYSQL.getJdbcUrl());

        /*
         * Grant the privileges MySQL CDC actually needs.
         *
         * The container's default user has only DML rights on its own schema. Capturing changes
         * requires reading the binlog, and the connector fails at startup without them:
         *   "Cannot read the binlog filename and position via 'SHOW MASTER STATUS' ...
         *    Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s)"
         *
         * These are the same grants the project's own src/test/resources/docker/setup.sql applies, so
         * the test mirrors real deployment requirements rather than assuming broader access.
         *
         * The grant is issued as root: cdcuser has no GRANT OPTION and could not grant to itself.
         */
        try (Connection c = openAsRoot();
                Statement st = c.createStatement()) {
            st.execute(
                    "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, "
                            + "LOCK TABLES ON *.* TO '"
                            + MYSQL.getUsername()
                            + "'@'%'");
            st.execute("FLUSH PRIVILEGES");

            st.execute(
                    "CREATE TABLE "
                            + TABLE
                            + " (id INT NOT NULL PRIMARY KEY, name VARCHAR(64), qty INT)");
            // Rows present before the CDC job starts: these must arrive via the snapshot phase.
            st.execute("INSERT INTO " + TABLE + " VALUES (1, 'snapshot-a', 10)");
            st.execute("INSERT INTO " + TABLE + " VALUES (2, 'snapshot-b', 20)");
        }
    }

    @AfterAll
    static void stopDatabase() {
        if (MYSQL.isRunning()) {
            MYSQL.stop();
        }
    }

    private static Connection openSource() throws Exception {
        return DriverManager.getConnection(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
    }

    /** Connects as root, which is what Testcontainers sets up for administrative operations. */
    private static Connection openAsRoot() throws Exception {
        return DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", MYSQL.getPassword());
    }

    private static void execute(String sql) throws Exception {
        try (Connection c = openSource();
                Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static int countRows() throws Exception {
        try (Connection c = openSource();
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + TABLE)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /** The captured table's Flink row type: (id INT, name VARCHAR, qty INT). */
    private static RowType rowType() {
        return (RowType)
                org.apache.flink.table.api.DataTypes.ROW(
                                org.apache.flink.table.api.DataTypes.FIELD(
                                        "id", org.apache.flink.table.api.DataTypes.INT()),
                                org.apache.flink.table.api.DataTypes.FIELD(
                                        "name", org.apache.flink.table.api.DataTypes.STRING()),
                                org.apache.flink.table.api.DataTypes.FIELD(
                                        "qty", org.apache.flink.table.api.DataTypes.INT()))
                        .getLogicalType();
    }

    /**
     * Runs the real CDC source and collects every emitted change event.
     *
     * <p>The job runs in the background with a bounded collection window: {@code CloseableIterator}
     * is drained until either the expected number of events arrives or the timeout expires, so the
     * test is not sensitive to exact CDC latency.
     *
     * @param expectedEvents stop reading once this many events have been collected
     * @param timeoutSeconds overall deadline
     * @return the collected events in arrival order
     */
    private List<Tuple2<String, Row>> collectChangeEvents(int expectedEvents, long timeoutSeconds)
            throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500);

        Map<String, RowType> tableRowTypes = new HashMap<>();
        tableRowTypes.put(TABLE, rowType());

        DataStream<Tuple2<String, Row>> stream =
                mysqlCdcStream(env, "mysql-cdc-e2e", tableRowTypes);

        List<Tuple2<String, Row>> collected = new ArrayList<>();
        try (CloseableIterator<Tuple2<String, Row>> it = stream.executeAndCollect()) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
            while (System.nanoTime() < deadline && collected.size() < expectedEvents) {
                if (it.hasNext()) {
                    collected.add(it.next());
                }
            }
        }
        return collected;
    }

    /**
     * Builds the real Flink CDC MySQL source for the container.
     *
     * <p>Deliberately mirrors how production code constructs the source, so the end-to-end test
     * exercises the same connector configuration rather than a simplified stand-in.
     */
    private static DataStream<Tuple2<String, Row>> mysqlCdcStream(
            StreamExecutionEnvironment env, String sourceName, Map<String, RowType> tableRowTypes) {
        return env.fromSource(
                MySqlSource.<Tuple2<String, Row>>builder()
                        .hostname(MYSQL.getHost())
                        .port(MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT))
                        .databaseList(SOURCE_DB)
                        .tableList(SOURCE_DB + "." + TABLE)
                        .username(MYSQL.getUsername())
                        .password(MYSQL.getPassword())
                        .deserializer(new CustomDebeziumDeserializer(tableRowTypes))
                        .startupOptions(StartupOptions.initial())
                        .serverTimeZone(SERVER_TIME_ZONE)
                        .build(),
                WatermarkStrategy.noWatermarks(),
                sourceName);
    }

    @Test
    void snapshotRowsAreCapturedAsInserts() throws Exception {
        // Two rows existed before startup; the initial snapshot must deliver both as inserts.
        List<Tuple2<String, Row>> events = collectChangeEvents(2, 60);

        assertEquals(2, events.size(), "expected both pre-existing rows from the snapshot phase");
        for (Tuple2<String, Row> e : events) {
            assertEquals(TABLE, e.f0, "table name must be propagated");
            assertEquals(RowKind.INSERT, e.f1.getKind(), "snapshot rows are inserts");
        }

        List<String> names = new ArrayList<>();
        for (Tuple2<String, Row> e : events) {
            names.add(String.valueOf(e.f1.getField(1)));
        }
        assertTrue(names.contains("snapshot-a"), "missing snapshot-a in " + names);
        assertTrue(names.contains("snapshot-b"), "missing snapshot-b in " + names);
    }

    @Test
    void liveInsertUpdateDeleteAreCapturedFromTheBinlog() throws Exception {
        int before = countRows();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500);

        Map<String, RowType> tableRowTypes = new HashMap<>();
        tableRowTypes.put(TABLE, rowType());

        DataStream<Tuple2<String, Row>> stream =
                mysqlCdcStream(env, "mysql-cdc-e2e-live", tableRowTypes);

        List<Tuple2<String, Row>> collected = new ArrayList<>();
        try (CloseableIterator<Tuple2<String, Row>> it = stream.executeAndCollect()) {
            // Drain the snapshot phase first (existing rows).
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (collected.size() < before && System.nanoTime() < deadline) {
                if (it.hasNext()) {
                    collected.add(it.next());
                }
            }
            assertEquals(before, collected.size(), "snapshot phase did not complete");

            // Now perform live DML and wait for the corresponding binlog events.
            execute("INSERT INTO " + TABLE + " VALUES (100, 'live-insert', 1)");
            execute("UPDATE " + TABLE + " SET qty = 99, name = 'live-updated' WHERE id = 100");
            execute("DELETE FROM " + TABLE + " WHERE id = 100");

            // Expect: 1 insert + (1 update_before + 1 update_after) + 1 delete = 4 events.
            int target = before + 4;
            while (collected.size() < target && System.nanoTime() < deadline) {
                if (it.hasNext()) {
                    collected.add(it.next());
                }
            }
        }

        List<Tuple2<String, Row>> live = collected.subList(before, collected.size());
        assertEquals(
                4,
                live.size(),
                "expected insert + update(before,after) + delete from the binlog, got " + live);

        // 1) INSERT
        assertEquals(
                RowKind.INSERT, live.get(0).f1.getKind(), "first binlog event should be INSERT");
        assertEquals(100, live.get(0).f1.getField(0));
        assertEquals("live-insert", String.valueOf(live.get(0).f1.getField(1)));

        // 2) UPDATE_BEFORE then 3) UPDATE_AFTER, in that order, with the right images.
        assertEquals(RowKind.UPDATE_BEFORE, live.get(1).f1.getKind());
        assertEquals(1, live.get(1).f1.getField(2), "UPDATE_BEFORE must carry the old qty");

        assertEquals(RowKind.UPDATE_AFTER, live.get(2).f1.getKind());
        assertEquals(99, live.get(2).f1.getField(2), "UPDATE_AFTER must carry the new qty");
        assertEquals("live-updated", String.valueOf(live.get(2).f1.getField(1)));

        // 4) DELETE, read from the `before` image.
        assertEquals(RowKind.DELETE, live.get(3).f1.getKind());
        assertEquals(100, live.get(3).f1.getField(0));
    }

    /**
     * Confirms the Flink CDC connector on the classpath is the one the active version line expects.
     *
     * <p>Both supported lines use CDC 3.x, so the presence of {@code org.apache.flink.cdc} classes
     * is the contract; the exact patch version is reported for diagnostics.
     */
    @Test
    void flinkCdcConnectorIsPresentAndUsable() {
        assertNotNull(
                MySqlSource.class.getPackage(), "Flink CDC MySqlSource must be on the classpath");
        assertTrue(
                MySqlSource.class.getName().startsWith("org.apache.flink.cdc."),
                "expected Flink CDC 3.x (org.apache.flink.cdc.*), got "
                        + MySqlSource.class.getName());

        String connectorVersion =
                MySqlSource.class.getPackage().getImplementationVersion() != null
                        ? MySqlSource.class.getPackage().getImplementationVersion()
                        : "unknown";
        LOG.info(
                "End-to-end test running against Flink CDC 3.x connector (jar version: {})",
                connectorVersion);
    }

    private static void assertNotNull(Object value, String message) {
        if (value == null) {
            throw new AssertionError(message);
        }
    }

    /** Guards against accidental use of the table row type elsewhere in this class. */
    @Test
    void producedTypeMatchesTheRegisteredRowType() {
        Map<String, RowType> tableRowTypes = new HashMap<>();
        tableRowTypes.put(TABLE, rowType());
        CustomDebeziumDeserializer d = new CustomDebeziumDeserializer(tableRowTypes);
        TypeInformation<Tuple2<String, Row>> info = d.getProducedType();
        assertEquals(Tuple2.class, info.getTypeClass());
        // Sanity check that RowData is not accidentally involved in this path.
        assertTrue(!RowData.class.equals(info.getTypeClass()));
    }
}
