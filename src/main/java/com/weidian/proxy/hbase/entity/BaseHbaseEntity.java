package com.weidian.proxy.hbase.entity;

import java.io.Serializable;

/**
 * Created by jiang on 17/10/30.
 */
public abstract class BaseHbaseEntity implements Serializable{

    protected String rowkey;
    protected long timestamp;

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
