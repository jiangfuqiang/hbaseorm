package com.weidian.proxy.hbase.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.weidian.proxy.hbase.annotation.ColName;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Created by jiang on 17/10/28.
 */
public class TestEntity extends BaseHbaseEntity {
    /**
     *
     */
    private static final long serialVersionUID = -4964149726553529181L;

    private long id;

    private long sellerId;

    private String buyerId;

    @ColName(name="ic")
    private Integer ic;

    private BigDecimal averageAmount = new BigDecimal(0);

    @ColName(name="purchase_amount")
    @JsonIgnore
    private BigDecimal totalAmount = new BigDecimal(0);


    @JsonProperty("total_amount")
    private BigDecimal purchaseAmount = new BigDecimal(0);

    private BigDecimal maxAmount = new BigDecimal(0);

    private int frequencyTrade;

    @JsonIgnore
    private int totalNum;
    @JsonProperty("totalNum")
    private int purchaseCount;

    private Date lastTime;

    private int cateTrade;

    private Date addTime;

    private Date updateTime;

    private int status;

    private int flagBin;

    private String extend1 = "";

    private String extend2 = "";

    private String extend3 = "";

    @JsonProperty("customer_type")
    private int customerType;
    private Integer readStatus;
    private Long lastPayDt;

    @JsonProperty("last_pay_time")
    private Long lastPayTime;

    private String JSON;
    @ColName(name="actionType")
    private Integer feedType;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getSellerId() {
        return sellerId;
    }

    public void setSellerId(long sellerId) {
        this.sellerId = sellerId;
    }

    public String getBuyerId() {
        return buyerId;
    }

    public void setBuyerId(String buyerId) {
        this.buyerId = buyerId;
    }

    public BigDecimal getAverageAmount() {
        return averageAmount;
    }

    public void setAverageAmount(BigDecimal averageAmount) {
        this.averageAmount = averageAmount;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public BigDecimal getMaxAmount() {
        return maxAmount;
    }

    public void setMaxAmount(BigDecimal maxAmount) {
        this.maxAmount = maxAmount;
    }

    public int getFrequencyTrade() {
        return frequencyTrade;
    }

    public void setFrequencyTrade(int frequencyTrade) {
        this.frequencyTrade = frequencyTrade;
    }

    public int getTotalNum() {
        return totalNum;
    }

    public void setTotalNum(int totalNum) {
        this.totalNum = totalNum;
    }

    public Date getLastTime() {
        return lastTime;
    }

    public void setLastTime(Date lastTime) {
        this.lastTime = lastTime;
    }

    public int getCateTrade() {
        return cateTrade;
    }

    public void setCateTrade(int cateTrade) {
        this.cateTrade = cateTrade;
    }

    public Date getAddTime() {
        return addTime;
    }

    public void setAddTime(Date addTime) {
        this.addTime = addTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getFlagBin() {
        return flagBin;
    }

    public void setFlagBin(int flagBin) {
        this.flagBin = flagBin;
    }

    public String getExtend1() {
        return extend1;
    }

    public void setExtend1(String extend1) {
        this.extend1 = extend1;
    }

    public String getExtend2() {
        return extend2;
    }

    public void setExtend2(String extend2) {
        this.extend2 = extend2;
    }

    public String getExtend3() {
        return extend3;
    }

    public void setExtend3(String extend3) {
        this.extend3 = extend3;
    }

    public int getCustomerType() {
        return customerType;
    }

    public void setCustomerType(int customerType) {
        this.customerType = customerType;
    }

    public Integer getReadStatus() {
        return readStatus;
    }

    public void setReadStatus(Integer readStatus) {
        this.readStatus = readStatus;
    }

    public Long getLastPayDt() {
        return lastPayDt;
    }

    public void setLastPayDt(Long lastPayDt) {
        this.lastPayDt = lastPayDt;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

    public Long getLastPayTime() {
        return lastPayTime;
    }

    public void setLastPayTime(Long lastPayTime) {
        this.lastPayTime = lastPayTime;
    }

    public BigDecimal getPurchaseAmount() {
        return purchaseAmount;
    }

    public void setPurchaseAmount(BigDecimal purchaseAmount) {
        this.purchaseAmount = purchaseAmount;
    }

    public int getPurchaseCount() {
        return purchaseCount;
    }

    public void setPurchaseCount(int purchaseCount) {
        this.purchaseCount = purchaseCount;
    }

    public String getJSON() {
        return JSON;
    }

    public void setJSON(String JSON) {
        this.JSON = JSON;
    }

    public int getFeedType() {
        return feedType;
    }

    public void setFeedType(int feedType) {
        this.feedType = feedType;
    }

    public Integer getIc() {
        return ic;
    }

    public void setIc(Integer ic) {
        this.ic = ic;
    }
}
