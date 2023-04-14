package io.sophiadata.flink;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * (@sophiadata) (@date 2023/4/13 17:07).
 */
public class KafkaDataGenerator {

    private static final String TOPIC_NAME = "test-data6";
    private static final int BATCH_SIZE = 10; // 每十条更改一次 TASK_ID 和 TEMPLATE_ID

    private final KafkaProducer<String, String> producer;
    private final Properties props;
    private int batchCount; // 计数器，用来判断是否达到更改 TASK_ID 和 TEMPLATE_ID 的条件

    public KafkaDataGenerator() {
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.153.201:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        batchCount = 0;
    }

    public void generateRandomData() {
        Faker faker = new Faker();
        String indCls = faker.number().digits(4) + "00"; // IND_CLS 不重复，可以用随机生成的四位数字加上 "00"
        String templateId = "20230210000012";
        if (batchCount == BATCH_SIZE) {
            // 达到更改 TASK_ID 和 TEMPLATE_ID 的条件，更新它们的值
            templateId = faker.number().digits(14);
            batchCount = 0;
        }
        int distLv = faker.number().numberBetween(1, 10); // 随机生成 1~10 之间的数字
        String taskId = "2023021000000080"; // 每十条变更一次 TASK_ID 和 TEMPLATE_ID，可以用计数器轮流生成两个值
        if (batchCount % 2 == 0) {
            taskId = "2023021000000079";
        }
        String dataDate = faker.date().future(365 * 10, java.util.concurrent.TimeUnit.DAYS).toString(); // 未来 10 年内的随机日期
        String calcTime = "2023-03-07 00:00:00.0"; // 近一年内的随机日期
        String indEnergy = String.format("%.4f", faker.number().randomDouble(4, 0, 1000)); // 随机生成 0~1000 之间的小数
        String[] columns = {"IND_CLS", "IND_ENERGY6", "IND_ENERGY5", "ENERGY_TYPE", "IND_ENERGY4",
                "IND_ENERGY3","IND_ENERGY2", "IND_ENERGY1", "INST_NUM", "TABLE_CODE", "DATA_DATE",
                "DATA_DATE_COLUMN", "CONS_NUM", "TEMPLATE_ID", "DIST_LV", "CALC_TIME", "MGT_ORG_CODE",
                "YN_FLAG", "PRO_MGT_ORG_CODE", "IND_ENERGY", "TASK_ID"};
        String[] values = {indCls, "0.0000", "0.0000", "02", "0.0000", "0.0000", "0.0000", "0.0000",
                null, "SGAMI_STAT.A_BA_IND_ENERGY_DAY2", dataDate, "", "1", templateId, String.format("%02d", distLv),
                calcTime, "3741602", "1", "37101", indEnergy, taskId};
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("\"").append(columns[i]).append("\":\"").append(values[i]).append("\"");
        }
        sb.append("}");

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, null, sb.toString());
        producer.send(record);

        batchCount++;
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) {
        KafkaDataGenerator generator = new KafkaDataGenerator();
        for (int i = 0; i < 100; i++) {
            generator.generateRandomData();
        }
        generator.close();
    }
}
