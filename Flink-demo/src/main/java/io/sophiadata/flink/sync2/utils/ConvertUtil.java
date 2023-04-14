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
package io.sophiadata.flink.sync2.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** (@SophiaData) (@date 2023/4/11 17:20). */
public class ConvertUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ConvertUtil.class);

    private static final String VARCHAR2 = "VARCHAR2";
    private static final String VARCHAR = "VARCHAR";
    private static final String NCHAR = "NCHAR";
    private static final String NUMBER = "NUMBER";
    private static final String DATE = "DATE";

    private static final String FLOAT = "FLOAT";

    private static final String CHAR = "CHAR";

    private static final String CLOB = "CLOB";
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";

    private static final String NCLOB = "NCLOB";

    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String BLOB = "BLOB";
    private static final String ROWID = "ROWID";
    private static final String INTERVAL_DAY_TO_SECOND = "INTERVAL DAY TO SECOND";

    public static DataType convertFromColumn(String column) {

        if (column.contains(VARCHAR2)) {
            return DataTypes.STRING();
        } else if (column.contains(NUMBER)) {
            return DataTypes.STRING();
        } else if (column.contains(DATE)) {
            return DataTypes.STRING();
        } else if (column.contains(CLOB)) {
            return DataTypes.STRING();
        } else if (column.contains(FLOAT)) {
            return DataTypes.FLOAT();
        } else if (column.contains(BLOB)) {
            return DataTypes.STRING();
        } else if (column.contains(TIMESTAMP)) {
            return DataTypes.STRING();
        } else if (column.contains(DOUBLE_PRECISION)) {
            return DataTypes.DOUBLE();
        } else {
            throw new UnsupportedOperationException(
                    String.format("Don't support Oracle type '%s' yet.", column));
        }
    }
}
