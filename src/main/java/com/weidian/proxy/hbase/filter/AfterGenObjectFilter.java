package com.weidian.proxy.hbase.filter;

/**
 * Created by jiang on 17/10/30.
 */
public interface AfterGenObjectFilter<T> {
    /**
     * 反射成对象之后过滤数据,返回false，数据将自动被过滤
     * @param rowkey
     * @param value
     * @return
     */
    public boolean filterAfterGenObject(String rowkey, T value);


}
