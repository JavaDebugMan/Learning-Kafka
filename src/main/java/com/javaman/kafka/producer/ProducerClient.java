package com.javaman.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author pengzhe
 * @date 2018/3/6 23:16
 * @description
 */

public class ProducerClient {

    /**
     * 主题名称
     */
    private static final String TOPIC = "test";
    /**
     * kafka集群
     */
    private static final String BROKER = "10.60.96.142:9092";

    private static KafkaProducer<String, String> producer = null;

    static {
        //构造用于实例化KafkaProducer的Properties
        Properties config = initConfig();
        //初始化一个KafkaProducer
        producer = new KafkaProducer<String, String>(config);
    }

    /**
     * 初始化kafka配置
     *
     * @return
     */
    private static Properties initConfig() {
        Properties properties = new Properties();
        //kafka broker列表
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        //设置序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    public static void main(String[] args) {
        try {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>
                        (TOPIC, "消息的key" + String.valueOf(i), "消息的value" + String.valueOf(i));
                Future<RecordMetadata> send = producer.send(record);
                RecordMetadata recordMetadata = send.get();
                System.out.println("此消息的Offset:::" + recordMetadata.offset() + "======此消息所发送的分区:::" + recordMetadata.partition());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
