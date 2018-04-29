package com.javaman.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * @author:彭哲
 * @Date:2017/12/16 消费者
 */
public class SingerConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        // properties.put("bootstrap.servers", "10.60.196.60:9092,10.60.196.64:9092,10.60.196.65:9092");
        // properties.put("group.id", "test");
        // properties.put("client.id", "test");
        // properties.put("fetch.max.bytes", 1024);
        //properties.put("enable.auto.commit", false);
        //properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


       // props.put("bootstrap.servers", "10.60.196.60:9092,10.60.196.64:9092,10.60.196.65:9092");
        props.put("bootstrap.servers", "10.60.196.225:9092,10.60.196.226:9092,10.60.196.227:9092");
        props.put("group.id", "test-consumer-group");
        props.put("auto.commit.timeout.interval.ms", 1000);
        props.put("session.timeout.ms", 30000);
        props.put("max.poll.records", 2000);
        props.put("enable.auto.commit", "false");//取消自动提交
      //  props.put("auto.offset.reset", "latest");//最新消息消费
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        consumer.subscribe(Arrays.asList("test"));

        try {
            //至少处理10条消息才提交Offset
            int minCommitSize = 1;
            int icount = 0;
            while (true) {
                //等待拉取消息
                ConsumerRecords<String, String> records = consumer.poll(10000);
                for (ConsumerRecord<String, String> record : records) {
                    //简单的打印出消息内容,模拟业务处理
                    System.out.printf("partition=%d,offset=%d,key=%s value=%s%n", record.partition()
                            , record.offset(), record.key(), record.value());
                    icount++;
                }
                //业务处理后提交偏移量
                if (icount >= minCommitSize) {
                    consumer.commitAsync(new OffsetCommitCallback() {
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            if (null == exception) {
                                //TODO 表示偏移量成功提交
                                System.out.println("提交成功");
                            } else {
                                //TODO 表示提交偏移量发生了异常,根据业务进行相关处理
                                System.out.println("发生了异常");
                            }
                        }
                    });
                    //重置计数器
                    icount = 0;
                }
            }
        } catch (Exception e) {
            //TODO 异常处理
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

}
