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
 * Version-neutral {@code ParameterTool} for code that must build against both Flink 1.20 and Flink
 * 2.x.
 *
 * <p>Flink 2.0 relocated {@code ParameterTool} from {@code org.apache.flink.api.java.utils} (in the
 * removed {@code flink-java} artifact) to {@code org.apache.flink.util} (in {@code flink-core}).
 * The API is identical; only the package changed. A package rename cannot be expressed as a Maven
 * property, so application code imports this class and the version-specific factory is chosen per
 * line via {@code src/main/flink20/java} and {@code src/main/flink2/java}.
 *
 * <p><strong>Why a wrapper rather than a subclass.</strong> Subclassing is impossible in practice:
 * the real {@code ParameterTool} declares only a {@code private} constructor and offers no
 * protected initialisation hook, so a subclass could not populate itself. Instead this delegates,
 * and callers that must hand Flink the real object use {@link #unwrap()} (or, where an equivalent
 * overload exists, {@link #toMap()}).
 *
 * <p>Only the surface this project actually uses is exposed, so an accidental new dependency on
 * Flink's parameter API shows up as a compile error rather than silently coupling the code to one
 * Flink version.
 */
public abstract class ParameterTool {

    /**
     * Returns the underlying Flink {@code ParameterTool}.
     *
     * <p>Required by {@link ExecutionConfig#setGlobalJobParameters}: the public overload takes an
     * {@code ExecutionConfig.GlobalJobParameters}, which the real {@code ParameterTool} extends,
     * and the {@code Map} overload is private. Callers cast the result to the version-appropriate
     * type via the helper below.
     *
     * @return the real Flink parameter tool for the active version line
     */
    public abstract ExecutionConfig.GlobalJobParameters asGlobalJobParameters();

    /** Creates a parameter tool from {@code --key value} program arguments. */
    public static ParameterTool fromArgs(String[] args) {
        return FactoryImpl.fromArgs(args);
    }

    /** Creates a parameter tool from a properties file path. */
    public static ParameterTool fromPropertiesFile(String path) throws IOException {
        return FactoryImpl.fromPropertiesFile(path);
    }

    /**
     * @return the value for {@code key}, or {@code defaultValue} when absent.
     */
    public abstract String get(String key, String defaultValue);

    /**
     * @return the value for {@code key}, or {@code null} when absent.
     */
    public abstract String get(String key);

    /**
     * @return the value for {@code key} as an int, or {@code defaultValue} when absent.
     */
    public abstract int getInt(String key, int defaultValue);

    /**
     * @return the value for {@code key} as an int.
     */
    public abstract int getInt(String key);

    /**
     * @return the value for {@code key} as a long, or {@code defaultValue} when absent.
     */
    public abstract long getLong(String key, long defaultValue);

    /**
     * @return the value for {@code key} as a boolean, or {@code defaultValue} when absent.
     */
    public abstract boolean getBoolean(String key, boolean defaultValue);

    /**
     * @return true when the given key is present.
     */
    public abstract boolean has(String key);

    /**
     * @return an immutable view of all parameters.
     */
    public abstract Map<String, String> toMap();

    /**
     * @return the number of parameters.
     */
    public abstract int getNumberOfParameters();

    /**
     * @return the parameters as {@link Properties}.
     */
    public abstract Properties getProperties();
}
