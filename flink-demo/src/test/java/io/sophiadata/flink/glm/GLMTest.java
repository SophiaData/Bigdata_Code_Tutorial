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

package io.sophiadata.flink.glm;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class GLMTest {

    private static final String GLM_API_URL =
            "https://open.bigmodel.cn/api/paas/v4/chat/completions";

    @Test
    public void testGLM() throws Exception {
        String apiKey = System.getenv("GLM_API_KEY");
        String model = System.getenv("GLM_MODEL_1");

        if (apiKey == null || model == null) {
            System.err.println("GLM_API_KEY or GLM_MODEL_1 not set");
            return;
        }

        System.out.println("Testing GLM API with model: " + model);
        System.out.println("API Key: " + apiKey.substring(0, 10) + "...");

        JsonObject requestBody = new JsonObject();
        requestBody.addProperty("model", model);

        JsonObject message = new JsonObject();
        message.addProperty("role", "user");
        message.addProperty("content", "你好，请简单介绍一下自己");

        com.google.gson.JsonArray messages = new com.google.gson.JsonArray();
        messages.add(message);
        requestBody.add("messages", messages);

        requestBody.addProperty("temperature", 0.7);
        requestBody.addProperty("max_tokens", 100);

        Gson gson = new Gson();
        String jsonStr = gson.toJson(requestBody);

        URL url = new URL(GLM_API_URL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Authorization", "Bearer " + apiKey);
        connection.setDoOutput(true);
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(30000);

        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = jsonStr.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        int responseCode = connection.getResponseCode();
        System.out.println("Response Code: " + responseCode);

        if (responseCode == HttpURLConnection.HTTP_OK) {
            try (BufferedReader br =
                    new BufferedReader(
                            new InputStreamReader(
                                    connection.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                System.out.println("Response: " + response.toString());

                JsonObject jsonResponse = gson.fromJson(response.toString(), JsonObject.class);
                if (jsonResponse.has("choices")) {
                    com.google.gson.JsonArray choices = jsonResponse.getAsJsonArray("choices");
                    if (choices.size() > 0) {
                        JsonObject choice = choices.get(0).getAsJsonObject();
                        JsonObject msg = choice.getAsJsonObject("message");
                        System.out.println("AI Response: " + msg.get("content").getAsString());
                    }
                }
            }
        } else {
            try (BufferedReader br =
                    new BufferedReader(
                            new InputStreamReader(
                                    connection.getErrorStream(), StandardCharsets.UTF_8))) {
                StringBuilder error = new StringBuilder();
                String errorLine;
                while ((errorLine = br.readLine()) != null) {
                    error.append(errorLine.trim());
                }
                System.err.println("Error Response: " + error.toString());
            }
        }

        connection.disconnect();
    }

    @Test
    public void testAllModels() throws Exception {
        String[] models = {
            System.getenv("GLM_MODEL_1"),
            System.getenv("GLM_MODEL_2"),
            System.getenv("GLM_MODEL_3"),
            System.getenv("GLM_MODEL_4")
        };

        String apiKey = System.getenv("GLM_API_KEY");
        if (apiKey == null) {
            System.err.println("GLM_API_KEY not set");
            return;
        }

        for (String model : models) {
            if (model != null && !model.isEmpty()) {
                System.out.println("\n=== Testing model: " + model + " ===");
                testModel(apiKey, model);
            }
        }
    }

    private void testModel(String apiKey, String model) throws Exception {
        JsonObject requestBody = new JsonObject();
        requestBody.addProperty("model", model);

        JsonObject message = new JsonObject();
        message.addProperty("role", "user");
        message.addProperty("content", "1+1等于几？");

        com.google.gson.JsonArray messages = new com.google.gson.JsonArray();
        messages.add(message);
        requestBody.add("messages", messages);

        requestBody.addProperty("temperature", 0.7);
        requestBody.addProperty("max_tokens", 50);

        Gson gson = new Gson();
        String jsonStr = gson.toJson(requestBody);

        URL url = new URL(GLM_API_URL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Authorization", "Bearer " + apiKey);
        connection.setDoOutput(true);
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(30000);

        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = jsonStr.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        int responseCode = connection.getResponseCode();
        System.out.println("Response Code: " + responseCode);

        if (responseCode == HttpURLConnection.HTTP_OK) {
            try (BufferedReader br =
                    new BufferedReader(
                            new InputStreamReader(
                                    connection.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                System.out.println("Response: " + response.toString());

                JsonObject jsonResponse = gson.fromJson(response.toString(), JsonObject.class);
                if (jsonResponse.has("choices")) {
                    com.google.gson.JsonArray choices = jsonResponse.getAsJsonArray("choices");
                    if (choices.size() > 0) {
                        JsonObject choice = choices.get(0).getAsJsonObject();
                        JsonObject msg = choice.getAsJsonObject("message");
                        System.out.println("AI Response: " + msg.get("content").getAsString());
                    }
                }
            }
        } else {
            try (BufferedReader br =
                    new BufferedReader(
                            new InputStreamReader(
                                    connection.getErrorStream(), StandardCharsets.UTF_8))) {
                StringBuilder error = new StringBuilder();
                String errorLine;
                while ((errorLine = br.readLine()) != null) {
                    error.append(errorLine.trim());
                }
                System.err.println("Error Response: " + error.toString());
            }
        }

        connection.disconnect();
    }
}
