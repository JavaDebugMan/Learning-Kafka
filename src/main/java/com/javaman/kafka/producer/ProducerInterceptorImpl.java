package com.javaman.kafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author pengzhe
 * @date 17/03/2018 12:48
 * @description 自定义ProducerInterceptor
 */

public class ProducerInterceptorImpl implements ProducerInterceptor<Integer, String> {


    /**
     * 在kafka生产者的消息发送到kafkaCluster前对消息进行过滤,过滤掉消息的key为偶数的消息
     *
     * @param record
     * @return
     */
    public ProducerRecord onSend(ProducerRecord<Integer, String> record) {
        if (record.key() % 2 == 0) {
            return record;
        }
        return null;
    }

    /**
     * 对于正常返回,Topic为test且分区编号为0的消息的返回的元数据进行输出
     *
     * @param metadata
     * @param exception
     */
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null && "test".equalsIgnoreCase(metadata.topic()) && metadata.partition() == 0) {
            System.out.println(metadata.toString());
        }

    }

    public void close() {

    }

    /**
     * 用来初始化此类的方法
     *
     * @param configs
     */
    public void configure(Map<String, ?> configs) {

    }
}
