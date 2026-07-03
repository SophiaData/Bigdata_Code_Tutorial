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

package io.sophiadata.flink.paimon.mongo;

import org.bson.Document;

import java.util.List;

/** Flattens MongoDB documents into Object arrays for Flink Row insertion. */
public final class MongoDocumentFlattener {

    private MongoDocumentFlattener() {}

    public static Object[] flatten(
            final Document doc, final String[] columns, final int flattenDepth) {
        Object[] result = new Object[columns.length];
        for (int i = 0; i < columns.length; i++) {
            result[i] = extractValue(doc, columns[i], flattenDepth);
        }
        return result;
    }

    private static Object extractValue(
            final Document doc, final String column, final int flattenDepth) {
        if (flattenDepth <= 0) {
            return convertValue(doc.get(column));
        }
        final String[] parts = column.split("\\.");
        Object current = doc;
        for (final String part : parts) {
            if (!(current instanceof Document)) {
                return convertValue(current);
            }
            current = ((Document) current).get(part);
            if (current == null) {
                return null;
            }
        }
        return convertValue(current);
    }

    private static Object convertValue(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Document) {
            return ((Document) value).toJson();
        }
        if (value instanceof List) {
            return value.toString();
        }
        return value;
    }
}
