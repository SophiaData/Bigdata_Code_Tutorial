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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Version-neutral replacement for Flink's {@code ParameterTool}.
 *
 * <p>Flink 2.0 removed the {@code flink-java} artifact and moved {@code ParameterTool} from {@code
 * org.apache.flink.api.java.utils} to {@code org.apache.flink.util}. The type itself is unchanged,
 * but a package rename cannot be expressed as a Maven property, so application code imports this
 * class instead. One implementation per version line is selected by a Maven source set:
 *
 * <ul>
 *   <li>{@code src/main/flink20/java} — against {@code org.apache.flink.api.java.utils}
 *   <li>{@code src/main/flink2/java} — against {@code org.apache.flink.util}
 * </ul>
 *
 * <p><strong>Why this extends {@code AbstractParameterTool}.</strong> {@code
 * ExecutionConfig#setGlobalJobParameters} accepts an {@code ExecutionConfig.GlobalJobParameters} and
 * its {@code Map} overload is private, so callers need a real subtype; a wrapper does not compile.
 * Extending Flink's {@code ParameterTool} is not possible either, because its only constructor is
 * private. {@code AbstractParameterTool} is the correct base: it is public, has a public no-arg
 * constructor, already extends {@code GlobalJobParameters}, and exposes the same abstract method set
 * on both Flink 1.20 and 2.x.
 */
public class ParameterTool extends org.apache.flink.util.AbstractParameterTool {

    private static final long serialVersionUID = 1L;

    private static final String NO_VALUE_KEY = "__NO_VALUE_KEY";

    private static final String DEFAULT_UNDEFINED = "\u0000";

    private final Map<String, String> data = new HashMap<>();

    /**
     * Creates an empty parameter tool.
     *
     * <p>The inherited {@code defaultData} and {@code unrequestedParameters} fields are initialised
     * here because Flink's own {@code ParameterTool} does so in a private constructor that a subclass
     * cannot call. Leaving them null makes the inherited {@code get(key, defaultValue)} and {@code
     * getUnrequestedParameters()} throw a NullPointerException.
     */
    public ParameterTool() {
        this.defaultData = new ConcurrentHashMap<>(0);
        this.unrequestedParameters = Collections.newSetFromMap(new ConcurrentHashMap<>(0));
    }

    /**
     * Creates a parameter tool backed by the given map.
     *
     * @param map the parameters to use directly
     * @return a tool containing {@code map}'s entries
     */
    public static ParameterTool fromMap(final Map<String, String> map) {
        ParameterTool tool = new ParameterTool();
        tool.data.putAll(map);
        return tool;
    }

    /**
     * Creates a parameter tool from {@code --key value} program arguments.
     *
     * @param args the program arguments
     * @return a tool containing the parsed arguments
     */
    public static ParameterTool fromArgs(final String[] args) {
        ParameterTool tool = new ParameterTool();
        tool.parseArgs(args);
        return tool;
    }

    /**
     * Creates a parameter tool from all system properties.
     *
     * @return a tool containing every system property
     */
    public static ParameterTool fromSystemProperties() {
        ParameterTool tool = new ParameterTool();
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            tool.data.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        return tool;
    }

    /**
     * Parses {@code --key value} pairs into this tool.
     *
     * <p>A trailing flag with no value is stored under {@code __NO_VALUE_KEY}, matching Flink's own
     * behaviour so downstream {@code get} calls behave identically.
     *
     * @param args the program arguments
     */
    public void parseArgs(final String[] args) {
        if (args == null) {
            return;
        }
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalStateException(
                        "Error parsing arguments. Expected a --key value pair but found: " + arg);
            }
            String key = arg.substring(2);
            int eq = key.indexOf('=');
            if (eq >= 0) {
                data.put(key.substring(0, eq), key.substring(eq + 1));
            } else {
                String value = NO_VALUE_KEY;
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    value = args[++i];
                }
                data.put(key, value);
            }
        }
    }

    /**
     * Merges another tool into this one; entries in {@code other} take precedence.
     *
     * @param other the tool to merge in
     * @return this tool, after merging
     */
    public ParameterTool mergeWith(ParameterTool other) {
        data.putAll(other.data);
        return this;
    }

    /**
     * Returns the value for a key, or {@code null} when absent.
     *
     * @param key the parameter name
     * @return the value, or {@code null}
     */
    /**
     * Returns the value for a key, or {@code null} when absent.
     *
     * <p>Declared {@code public} because Flink's own {@code ParameterTool} widens the abstract {@code
     * protected} declaration; callers such as NacosUtil rely on the public form.
     *
     * @param key the parameter name
     * @return the value, or {@code null}
     */
    @Override
    public String get(String key) {
        return data.get(key);
    }

    @Override
    public boolean has(String key) {
        return data.containsKey(key);
    }

    @Override
    protected int getNumberOfParameters() {
        return data.size();
    }

    @Override
    public Map<String, String> toMap() {
        return Collections.unmodifiableMap(data);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        ParameterTool copy = new ParameterTool();
        copy.data.putAll(this.data);
        return copy;
    }
}