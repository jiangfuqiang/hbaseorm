package com.weidian.proxy.hbase.filter;

import net.sf.json.JSONObject;

/**
 * Created by jiang on 17/10/30.
 */
public interface HbaseFilter<T> {

    /**
     * value是普通值
     * @param colFamily
     * @param colName
     * @param value
     * @return
     */
    public T filterValue(String colFamily,String colName, T value);

    /**
     * value是json格式
     * @param colFamily
     * @param colName
     * @param value
     * @return
     */
    public JSONObject filterJsonValue(String colFamily,String colName, JSONObject value);


}
