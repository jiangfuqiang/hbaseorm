package com.weidian.proxy.hbase.spring;

import com.weidian.proxy.hbase.common.BaseHbaseDao;
import com.weidian.proxy.hbase.entity.BaseHbaseEntity;
import com.weidian.proxy.hbase.entity.RowKeyEntity;
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
     * @param rowPrifix
     * @param startRowKey
     * @param offset
     * @return
     */
    public List<T> scanDataByRowkeysWithFilter(final Class clazz,
                                     final String tableName,
                                     final String rowPrifix, final String startRowKey, final Long offset,
                                               final List<Filter> outFilters) {
        List<T> objects = new ArrayList<T>(offset.intValue());
        objects = hbaseTemplate.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(HTableInterface hTableInterface) throws Throwable {
                List<T> objects = new ArrayList<T>(offset.intValue());
                List<Filter> filters = new ArrayList<>(2);
                filters.add(new PrefixFilter(rowPrifix.getBytes()));
                if(offset > 0) {
                    filters.add(new PageFilter(offset));
                }
                if(outFilters != null && outFilters.size() > 0) {
                    filters.addAll(outFilters);
                }
                FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

                Scan s = new Scan();
                s.setStartRow(Bytes.toBytes(startRowKey));
                s.setFilter(list);
                s.setCaching(100);
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
     * @param tableName
     * @param rowPrifix
     * @param startRowKey
     * @param offset
     * @return
     */
    public List<T> scanDataByRowkeys(final Class clazz,
                                                   final String tableName,
                                                   final String rowPrifix, final String startRowKey, final Long offset) {
        List<T> objects = new ArrayList<T>(offset.intValue());
        objects = hbaseTemplate.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(HTableInterface hTableInterface) throws Throwable {
                List<T> objects = new ArrayList<T>(offset.intValue());
                List<Filter> filters = new ArrayList<>(2);
                filters.add(new PrefixFilter(rowPrifix.getBytes()));
                if(offset > 0) {
                    filters.add(new PageFilter(offset));
                }
                FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

                Scan s = new Scan();
                s.setStartRow(Bytes.toBytes(startRowKey));
                s.setFilter(list);
                s.setCaching(100);
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
     * @param tableName
     * @param rowPrifix
     * @param startRowKey
     * @param offset
     * @return
     */
    public List<T> scanDataByRowkeysWithFilters(final Class clazz,
                                                final String tableName,
                                                final String rowPrifix, final String startRowKey, final Long offset,
                                                final List<Filter> outFilters) {
        List<T> objects = new ArrayList<T>(offset.intValue());
        objects = hbaseTemplate.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(HTableInterface hTableInterface) throws Throwable {
                List<T> objects = new ArrayList<T>(offset.intValue());
                List<Filter> filters = new ArrayList<>(2);
                filters.add(new PrefixFilter(rowPrifix.getBytes()));
                if(offset > 0) {
                    filters.add(new PageFilter(offset));
                }
                if(outFilters != null && outFilters.size() > 0) {
                    filters.addAll(outFilters);
                }
                FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

                Scan s = new Scan();
                s.setStartRow(Bytes.toBytes(startRowKey));
                s.setFilter(list);
                s.setCaching(100);
                ResultScanner rs = hTableInterface.getScanner(s);

                parseHbaseResult(rs,objects,startRowKey,clazz);

                rs.close();

                return objects;
            }
        });
        return objects;
    }

    /**
     * scan的范围查询
     * @param clazz
     * @param tableName
     * @param columnFamily
     * @param startRowKey
     * @param endRowKey
     * @param limit
     * @return
     */
    public List<T> scanDataRangeByRowkeys(final Class clazz,
                                     final String tableName, final String columnFamily,
                                     final String startRowKey, final String endRowKey, final Long limit) {
        List<T> objects = new ArrayList<T>(limit.intValue());
        objects = hbaseTemplate.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(HTableInterface hTableInterface) throws Throwable {
                List<T> objects = new ArrayList<T>(limit.intValue());


                Scan s = new Scan();
                s.setStartRow(Bytes.toBytes(startRowKey));
                s.setStopRow(Bytes.toBytes(endRowKey));
                if(limit > 0) {
                    s.setFilter(new PageFilter(limit));
                }
                s.setCaching(100);
                ResultScanner rs = hTableInterface.getScanner(s);

                parseHbaseResult(rs,objects,startRowKey,clazz);

                rs.close();

                return objects;
            }
        });
        return objects;
    }

    /**
     * scan的范围查询
     * @param clazz
     * @param tableName
     * @param columnFamily
     * @param startRowKey
     * @param endRowKey
     * @param limit
     * @return
     */
    public List<T> scanDataRangeByRowkeysWithFilters(final Class clazz,
                                          final String tableName, final String columnFamily,
                                          final String startRowKey, final String endRowKey, final Long limit,
                                                    final List<Filter> outFilters) {
        List<T> objects = new ArrayList<T>(limit.intValue());
        objects = hbaseTemplate.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(HTableInterface hTableInterface) throws Throwable {
                List<T> objects = new ArrayList<T>(limit.intValue());


                List<Filter> filters = new ArrayList<>(2);
                if(limit > 0) {
                    filters.add(new PageFilter(limit));
                }
                if(outFilters != null && outFilters.size() > 0) {
                    filters.addAll(outFilters);
                }
                FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
                Scan s = new Scan();
                s.setStartRow(Bytes.toBytes(startRowKey));
                s.setStopRow(Bytes.toBytes(endRowKey));
                s.setFilter(list);
                s.setCaching(100);
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
            if(!rowkey.startsWith(startRowKey)) {
                continue;
            }
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
