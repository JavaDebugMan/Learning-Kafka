package com.javaman.kafka.producer;

import com.javaman.kafka.bean.StockQuotationInfo;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.DecimalFormat;
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
        Random r = new Random();
        Integer stockCode = r.nextInt(10) + 60010;
        //产生一个0~1之间的随机浮点数
        float random = (float) Math.random();
        //设置涨跌规则
        if (random / 2 < 0.5) {
            random = -random;
        }
        //保存两位有效数字
        DecimalFormat decimalFormat = new DecimalFormat(".00");
        //设置最新价在11元浮动
        quotationInfo.setCurrentPrice(Float.valueOf(decimalFormat.format(11 + random)));
        //设置昨日收盘价为固定值
        quotationInfo.setPreClosePrice(11.80f);
        //设置开盘价
        quotationInfo.setOpenPrce(11.5f);
        //设置最低价,并不考虑10%限制,以及当前价是否已是最低价
        quotationInfo.setLowPrice(10.5f);
        //设置最高价,并不考虑10%限制,以及当前价是否已是最高价
        quotationInfo.setHighPrice(12.5f);
        quotationInfo.setStockCode(stockCode.toString());
        quotationInfo.setStockName("股票--" + stockCode);
        return quotationInfo;
    }

    public static void main(String[] args) {
        ProducerRecord<String, String> record = null;
        StockQuotationInfo quotationInfo = null;
        try {
            int num = 0;
            for (int i = 0; i < MSG_SIZE; i++) {
                quotationInfo = createQuotationInfo();
                record = new ProducerRecord<String, String>(TOPIC, null,
                        quotationInfo.getTradeTime(), quotationInfo.getStockCode(),
                        quotationInfo.toString());
                //异步发送消息
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            //发送异常记录异常信息
                            System.out.println("发送消息回调异常:Send message occurs exception," + exception);
                        }
                        if (metadata != null) {
                            System.out.println("offset" + metadata.offset());
                        }

                    }
                });
            }
        } catch (Exception e) {
            System.out.println("生产者异常:Send message occurs exception," + e);
        } finally {
            producer.close();
        }
    }
}
