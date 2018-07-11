package com.rox.kafka;

import kafka.admin.TopicCommand;

public class KafkaUtil {

    public static void main(String[] args) {

        createTopic();

    }

    public static void createTopic() {
        /**
         * 创建一个主题  topic
         */

        String[] ops = new String[] {
                "--create",
                "--zookeeper",
                "cs1:2181,cs2:2181,cs3:2181",
                "--replication-factor",
                "3",
                "--partitions",
                "10",
                "--topic",
                "kafka_test" };

        TopicCommand.main(ops);

    }
}
