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

package io.sophiadata.flink.source.utils;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/** (@sophiadata) (@date 2023/8/2 11:14). */
public class ConfigUtil {

    public static String loadJsonFile(String fileName) {

        String filePath = getJarDir() + "/" + fileName;
        //  System.out.println(filePath);
        File file = new File(filePath);
        InputStream resourceAsStream = null;
        try {
            if (file.exists()) {
                resourceAsStream = new FileInputStream(file);
            } else {
                resourceAsStream =
                        Thread.currentThread()
                                .getContextClassLoader()
                                .getResourceAsStream(fileName);
            }
            String json = IOUtils.toString(resourceAsStream, "utf-8");
            return json;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("配置文件" + fileName + "读取异常");
        }
    }

    public static String getJarDir() {
        File file = getJarFile();
        if (file == null) {
            return null;
        }
        return file.getParent();
    }

    private static File getJarFile() {

        //        ApplicationHome h = new ApplicationHome(ConfigUtil.class);
        //        File jarF = h.getSource();
        //        System.out.println(jarF.getParentFile().toString());
        //        return jarF;

        String path =
                ConfigUtil.class.getProtectionDomain().getCodeSource().getLocation().getFile();
        try {
            path = java.net.URLDecoder.decode(path, "UTF-8"); // 转换处理中文及空格
        } catch (java.io.UnsupportedEncodingException e) {
            return null;
        }
        return new File(path);
    }
}
