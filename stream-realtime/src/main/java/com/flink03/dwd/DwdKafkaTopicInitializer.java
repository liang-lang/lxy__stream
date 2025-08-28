package com.flink03.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;

/**
 * @Description: TODO DWD层Kafka主题初始化类 用于创建DWD层所需的Kafka主题
 */


public class DwdKafkaTopicInitializer {

    public static void createDwdTopics() {
        String bootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");

        // 创建DWD层Kafka主题

        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.user.visit.log.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.product.interaction.log.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.order.pay.log.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.search.log.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.page.click.log.topic"), 2, (short) 1, false);
        KafkaUtils.createKafkaTopic(bootstrapServers, ConfigUtils.getString("kafka.ods.crowd.attribute.log.topic"), 2, (short) 1, false);

        System.out.println("DWD层Kafka主题创建完成");
    }

    public static void main(String[] args) {
        createDwdTopics();
    }
}