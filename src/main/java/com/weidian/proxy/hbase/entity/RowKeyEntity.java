package com.weidian.proxy.hbase.entity;

/**
 * Created by jiang on 17/10/30.
 */
public class RowKeyEntity {

    //表名
    private String tbl;

    private String rowkey;

    //列簇
    private String cf;

    //列名
    private String qualifier;

    public String getTbl() {
        return tbl;
    }

    public void setTbl(String tbl) {
        this.tbl = tbl;
    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getCf() {
        return cf;
    }

    public void setCf(String cf) {
        this.cf = cf;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }
}
