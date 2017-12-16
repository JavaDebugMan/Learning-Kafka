package com.javaman.kafka.bean;

import java.io.Serializable;

/**
 * @author:彭哲
 * @Date:2017/12/16 kafka发送的JavaBean
 */
public class StockQuotationInfo implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * 股票代码
     */
    private String stockCode;
    /**
     * 股票名称
     */
    private String stockName;
    /**
     * 交易时间
     */
    private long tradeTime;
    /**
     * 昨日收盘价
     */
    private float preClosePrice;
    /**
     * 开盘价
     */
    private float openPrce;
    /**
     * 当前价,收盘时即为当日收盘价
     */
    private float currentPrice;
    /**
     * 今日最高价
     */
    private float highPrice;
    /**
     * 今日最低价
     */
    private float lowPrice;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getStockCode() {
        return stockCode;
    }

    public void setStockCode(String stockCode) {
        this.stockCode = stockCode;
    }

    public String getStockName() {
        return stockName;
    }

    public void setStockName(String stockName) {
        this.stockName = stockName;
    }

    public long getTradeTime() {
        return tradeTime;
    }

    public void setTradeTime(long tradeTime) {
        this.tradeTime = tradeTime;
    }

    public float getPreClosePrice() {
        return preClosePrice;
    }

    public void setPreClosePrice(float preClosePrice) {
        this.preClosePrice = preClosePrice;
    }

    public float getOpenPrce() {
        return openPrce;
    }

    public void setOpenPrce(float openPrce) {
        this.openPrce = openPrce;
    }

    public float getCurrentPrice() {
        return currentPrice;
    }

    public void setCurrentPrice(float currentPrice) {
        this.currentPrice = currentPrice;
    }

    public float getHighPrice() {
        return highPrice;
    }

    public void setHighPrice(float highPrice) {
        this.highPrice = highPrice;
    }

    public float getLowPrice() {
        return lowPrice;
    }

    public void setLowPrice(float lowPrice) {
        this.lowPrice = lowPrice;
    }

    @Override
    public String toString() {
        return "StockQuotationInfo{" +
                "stockCode='" + stockCode + '\'' +
                ", stockName='" + stockName + '\'' +
                ", tradeTime=" + tradeTime +
                ", preClosePrice=" + preClosePrice +
                ", openPrce=" + openPrce +
                ", currentPrice=" + currentPrice +
                ", highPrice=" + highPrice +
                ", lowPrice=" + lowPrice +
                '}';
    }
}
