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

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

/**
 * Flink 2.x flavour of the parallel source contract.
 *
 * <p><strong>This is a {@code flink2} source-set file.</strong> Flink 2.0 removed {@code
 * ParallelSourceFunction} and relocated the surviving legacy interface to {@code
 * org.apache.flink.streaming.api.functions.source.legacy.SourceFunction}. The legacy API is still
 * usable through {@code StreamExecutionEnvironment#addSource}, but it no longer offers a
 * parallelism hint; sources that need parallelism above one must be migrated to the FLIP-27 {@code
 * Source} API.
 *
 * @param <T> the emitted type
 */
public interface MockSource<T> extends SourceFunction<T> {}