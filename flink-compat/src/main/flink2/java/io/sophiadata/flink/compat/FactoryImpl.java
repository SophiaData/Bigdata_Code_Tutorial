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

package io.sophiadata.flink.compat;

import org.apache.flink.api.common.ExecutionConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Flink 2.x implementation of the {@link ParameterTool} shim.
 *
 * <p><strong>This is a {@code flink2} source-set file</strong>, compiled only on the Flink 2.x line,
 * where {@code ParameterTool} moved to {@code flink-core} under {@code org.apache.flink.util} (the
 * {@code flink-java} artifact it used to live in was removed in Flink 2.0). The {@code flink20}
 * source set provides the mirror image against {@code org.apache.flink.api.java.utils} for Flink
 * 1.20.
 */
final class FactoryImpl {

    private FactoryImpl() {}

    static ParameterTool fromArgs(String[] args) {
        return new Impl(org.apache.flink.util.ParameterTool.fromArgs(args));
    }

    static ParameterTool fromPropertiesFile(String path) throws IOException {
        return new Impl(org.apache.flink.util.ParameterTool.fromPropertiesFile(path));
    }

    /** Wraps the Flink 2.x {@code ParameterTool}. */
    private static final class Impl extends ParameterTool {

        private final org.apache.flink.util.ParameterTool delegate;

        Impl(org.apache.flink.util.ParameterTool delegate) {
            this.delegate = delegate;
        }

        @Override
        public ExecutionConfig.GlobalJobParameters asGlobalJobParameters() {
            // The real ParameterTool extends GlobalJobParameters on this line.
            return delegate;
        }

        @Override
        public String get(String key, String defaultValue) {
            return delegate.get(key, defaultValue);
        }

        @Override
        public String get(String key) {
            return delegate.get(key);
        }

        @Override
        public int getInt(String key, int defaultValue) {
            return delegate.getInt(key, defaultValue);
        }

        @Override
        public int getInt(String key) {
            return delegate.getInt(key);
        }

        @Override
        public long getLong(String key, long defaultValue) {
            return delegate.getLong(key, defaultValue);
        }

        @Override
        public boolean getBoolean(String key, boolean defaultValue) {
            return delegate.getBoolean(key, defaultValue);
        }

        @Override
        public boolean has(String key) {
            return delegate.has(key);
        }

        @Override
        public Map<String, String> toMap() {
            return delegate.toMap();
        }

        @Override
        public int getNumberOfParameters() {
            return delegate.getNumberOfParameters();
        }

        @Override
        public Properties getProperties() {
            return delegate.getProperties();
        }
    }
}
