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

package io.sophiadata.flink.sync.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** (@SophiaData) (@date 2023/5/15 21:53). */
public class UpDataDataUtil {

    private static PreparedStatement stmt;
    private static final Logger logger = LoggerFactory.getLogger(UpDataDataUtil.class);

    public static void updateTaskStatus(
            String kafkaCount,
            Connection oracleConnection,
            String taskId,
            String proMgtOrgCode,
            String taskStatus) {

        String reportSql2 = null;
        try {
            if (kafkaCount != null) {
                reportSql2 =
                        "UPDATE SGAMI_HEAD_OPERATION.A_BMCM_EXEC_DET SET EXEC_RS =? WHERE TASK_ID = ? AND AREA_CODE = ?";
            } else {
                if (!taskStatus.equals("08") && !taskStatus.equals("09")) {
                    reportSql2 =
                            "UPDATE SGAMI_HEAD_OPERATION.A_DG_ORG_TASK_MONITOR SET ORG_STATUS =?  WHERE TASK_ID = ? AND PRO_MGT_ORG_CODE = ?";
                }
            }
            if (reportSql2 != null) {
                stmt = oracleConnection.prepareStatement(reportSql2);
                stmt.setString(1, taskStatus);
                stmt.setString(2, taskId);
                stmt.setString(3, proMgtOrgCode);
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            logger.error(" SQLException -> {}", e.getMessage());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.error("stmt close error {}", e.getMessage());
                }
            }
        }
    }

    public static void updateReadKafka(
            String kafkaCount,
            Connection oracleConnection,
            String taskId,
            String proMgtOrgCode,
            Long readKafkaNum) {

        String reportSql2;
        if (!kafkaCount.equals("null")) {
            reportSql2 =
                    "UPDATE SGAMI_HEAD_OPERATION.A_BMCM_EXEC_DET SET READ_KAFKA_NUM =? WHERE TASK_ID = ? AND AREA_CODE = ?";

        } else {
            reportSql2 =
                    "UPDATE SGAMI_HEAD_OPERATION.A_DG_ORG_TASK_MONITOR SET READ_KAFKA_NUM =?  WHERE TASK_ID = ? AND PRO_MGT_ORG_CODE = ?";
        }
        try {
            stmt = oracleConnection.prepareStatement(reportSql2);
            stmt.setLong(1, readKafkaNum);
            stmt.setString(2, taskId);
            stmt.setString(3, proMgtOrgCode);
            stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error(" SQLException -> {}", e.getMessage());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.error("stmt close error {}", e.getMessage());
                }
            }
        }
    }
}
