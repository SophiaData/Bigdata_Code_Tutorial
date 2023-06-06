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

package test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.Random;

/** (@gtk) (@date 2023/6/5 17:51). */
public class InsertSqlGenerator {

    private static final String INSERT_TEMPLATE =
            "INTO SGAMI_STAT.A_BA_IND_ENERGY_DAY2 (IND_CLS2,IND_ENERGY6,IND_ENERGY5,ENERGY_TYPE2,IND_ENERGY4,IND_ENERGY3,IND_ENERGY2,IND_ENERGY1,INST_NUM2,DATA_DATE)  values('%s','0.0000','0.0000','0.0000','0.0000','0.0000','0.0000','0.0000','0.0000',TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS')) LOG ERRORS INTO SGAMI_STAT.MY_ERROR_INFO ('insert') REJECT LIMIT UNLIMITED";

    public static void main(String[] args) {
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        sb.append("INSERT ALL ");
        for (int i = 1800; i < 2300; i++) {
            String indCls2 = String.format("%04d", random.nextInt(10000));
            String dataDate = new Timestamp(System.currentTimeMillis()).toString().substring(0, 19);
            String insert = String.format(INSERT_TEMPLATE, indCls2, dataDate);
            sb.append(insert).append("\n");
        }
        sb.append(" SELECT 1 FROM dual ");
        File file = new File("output.sql"); // 创建文件对象
        try {
            PrintWriter writer = new PrintWriter(file); // 创建PrintWriter对象
            writer.println(sb); // 将内容写入文件
            writer.close(); // 关闭PrintWriter对象
            System.out.println("输出成功！");
        } catch (FileNotFoundException e) {
            System.out.println("文件不存在！");
        }
        //        System.out.println(sb);
    }
}
