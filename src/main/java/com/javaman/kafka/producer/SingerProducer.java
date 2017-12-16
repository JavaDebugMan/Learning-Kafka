package com.javaman.kafka.producer;

import com.javaman.kafka.bean.StockQuotationInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * @author:彭哲
 * @Date:2017/12/16 单线程生产者
 */
public class SingerProducer {

    /**
     * 设置实例生产的消息总数
     */
    private static final int MSG_SIZE = 100;
    /**
     * 主题名称
     */
    private static final String TOPIC = "stock-quotation";
    /**
     * kafka集群
     */
    private static final String BROKER_LIST = "119.23.236.253:9092";

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
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        //设置序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static StockQuotationInfo createQuotationInfo() {
        StockQuotationInfo quotationInfo = new StockQuotationInfo();
        //随机产生1到10的整数,然后与600100相加组成股票代码
        Random random = new Random();
        Integer stockCode = random.nextInt(10) + 60010;
        return quotationInfo;
    }


}
