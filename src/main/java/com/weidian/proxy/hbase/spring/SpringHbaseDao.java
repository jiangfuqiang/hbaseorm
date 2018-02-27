package com.weidian.proxy.hbase.spring;

import com.weidian.proxy.hbase.common.BaseHbaseDao;
import com.weidian.proxy.hbase.entity.BaseHbaseEntity;
import com.weidian.proxy.hbase.entity.RowKeyEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiang on 17/10/28.
 */
@Repository
public  class SpringHbaseDao<T> extends BaseHbaseDao<T>{

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringHbaseDao.class);

    private HbaseTemplate hbaseTemplate;

    /**
     * Hbase的get请求
     * @param clazz
     * @param rowkeys
     * @param tableName
     * @return
     */
    public List<T> getDataByRowkeys(final Class clazz,
                                                  final List<RowKeyEntity> rowkeys,
                                                  final String tableName) {
        List<T> objects = new ArrayList<T>(rowkeys.size());
        objects = hbaseTemplate.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(HTableInterface hTableInterface) throws Throwable {
                List<T> objects = new ArrayList<T>(rowkeys.size());
                for(RowKeyEntity rowKeyEntity : rowkeys) {
                    String rowkey = rowKeyEntity.getRowkey();
                    Get get = new Get(Bytes.toBytes(rowkey));

                    Result result = hTableInterface.get(get);
                    if (result == null || result.listCells() == null) {
                        LOGGER.error("not found data：" + rowkey);
                        continue;
                    }

                    Object object = convertToEntity(rowkey,clazz,result);
                    if(object == null) {
                        LOGGER.error("can't new instance object: " + clazz);
                        continue;
                    }

                    objects.add((T)object);
                }

                return objects;
            }
        });
        return objects;
    }

    /**
     * scan的分页查询
     * @param clazz
     * @param tableName
     * @param startRowKey
     * @return
     */
    public List<T> scanDataByRowkeysWithFilter(final Class clazz,
                                     final String tableName,
                                     final String startRowKey,final String prefixRowkey,
                                               final Long limit,
                                               final List<Filter> outFilters,boolean isDesc) {
        if(isDesc && StringUtils.isBlank(prefixRowkey)) {
            throw new IllegalArgumentException("prefix rowkey must not empty with desc is true");
        }
        List<T> objects = new ArrayList<T>();
        objects = hbaseTemplate.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(HTableInterface hTableInterface) throws Throwable {
                List<T> objects = new ArrayList<T>();
                List<Filter> filters = new ArrayList<>(2);
                filters.add(new PageFilter(limit));

                if(outFilters != null && outFilters.size() > 0) {
                    filters.addAll(outFilters);
                }
                FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

                Scan s = new Scan();
                s.setStartRow(Bytes.toBytes(startRowKey));
                s.setFilter(list);

                ResultScanner rs = hTableInterface.getScanner(s);

                parseHbaseResult(rs,objects,startRowKey,clazz);

                rs.close();

                return objects;
            }
        });
        return objects;
    }

    /**
     * scan的分页查询
     * @param clazz
     * @param tableN
     * @param maxRowKey
     * @param limit
     * @return
     */
    private List<T> scanDataByRowkeys(final Class clazz,
                                      final String tableN,
                                      final String maxRowKey, String prefixRowkey,final Long limit, boolean isDesc) throws Exception{
        return scanDataByRowkeysWithFilter(clazz, tableN, maxRowKey, prefixRowkey, limit,null, isDesc);
    }

    /**
     * scan的分页查询
     * @param clazz
     * @param tableN
     * @param stopRowkey
     * @param limit
     * @return
     */
    public List<T> scanDataByDescRowkeys(final Class clazz,
                                         final String tableN,
                                         final String stopRowkey, String prefixRowkey,final Long limit) throws Exception{
        if(StringUtils.isBlank(prefixRowkey)) {
            throw new IllegalArgumentException("prefix rowkey must not empty");
        }


        return this.scanDataByRowkeys(clazz,tableN,stopRowkey,prefixRowkey,limit,true);
    }

    /**
     * scan的分页查询
     * @param clazz
     * @param tableN
     * @param stopRowkey
     * @param limit
     * @return
     */
    public List<T> scanDataByAscRowkeys(final Class clazz,
                                        final String tableN,
                                        final String stopRowkey, String prefixRowkey,final Long limit) throws Exception{

        return this.scanDataByRowkeys(clazz,tableN,stopRowkey,prefixRowkey,limit,false);
    }

    /**
     * scan的分页查询
     * @param clazz
     * @param tableN
     * @param stopRowkey
     * @param limit
     * @return
     */
    public List<T> scanDataByAscRowkeys(final Class clazz,
                                        final String tableN,
                                        final String stopRowkey,final Long limit) throws Exception{

        return this.scanDataByRowkeys(clazz,tableN,stopRowkey,"",limit,false);
    }



    /**
     * scan的范围查询
     * @param clazz
     * @param tableName
     * @param startRowKey
     * @param endRowKey
     * @return
     */
    public List<T> scanDataRangeByRowkeys(final Class clazz,
                                     final String tableName,
                                     final String startRowKey, final String endRowKey) {
       return scanDataRangeByRowkeysWithFilters(clazz,tableName,startRowKey,endRowKey,null);
    }

    /**
     * scan的范围查询
     * @param clazz
     * @param tableName
     * @param startRowKey
     * @param endRowKey
     * @return
     */
    public List<T> scanDataRangeByRowkeysWithFilters(final Class clazz,
                                          final String tableName,
                                          final String startRowKey, final String endRowKey,
                                                    final List<Filter> outFilters) {
        List<T> objects = new ArrayList<T>();
        objects = hbaseTemplate.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(HTableInterface hTableInterface) throws Throwable {
                List<T> objects = new ArrayList<T>();


                List<Filter> filters = new ArrayList<>(2);

                if(outFilters != null && outFilters.size() > 0) {
                    filters.addAll(outFilters);
                }
                FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
                Scan s = new Scan();
                s.setStartRow(Bytes.toBytes(startRowKey));
                s.setStopRow(Bytes.toBytes(endRowKey));
                s.setFilter(list);
                ResultScanner rs = hTableInterface.getScanner(s);

                parseHbaseResult(rs,objects,startRowKey,clazz);

                rs.close();

                return objects;
            }
        });
        return objects;
    }

    /**
     * 解析hbase的result，取出数据
     * @param rs
     * @param objects
     * @param startRowKey
     * @param clazz
     */
    private void parseHbaseResult(ResultScanner rs,List<T> objects,String startRowKey,Class clazz) {
        for(Result result : rs) {
            String rowkey = Bytes.toString(result.getRow());
            if (result == null || result.listCells() == null) {
                LOGGER.error("not found data：" + rowkey);
                continue;
            }
//            if(!rowkey.startsWith(startRowKey)) {
//                continue;
//            }
            Object object = convertToEntity(rowkey,clazz,result);
            if(object == null) {
                LOGGER.error("can't new instance object: " + clazz);
                continue;
            }

            objects.add((T)object);
        }
    }

    public HbaseTemplate getHbaseTemplate() {
        return hbaseTemplate;
    }

    public void setHbaseTemplate(HbaseTemplate hbaseTemplate) {
        this.hbaseTemplate = hbaseTemplate;
    }
}
