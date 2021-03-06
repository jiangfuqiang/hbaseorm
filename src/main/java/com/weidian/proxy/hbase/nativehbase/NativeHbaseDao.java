package com.weidian.proxy.hbase.nativehbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.weidian.proxy.hbase.common.BaseHbaseDao;
import com.weidian.proxy.hbase.common.HbaseConnectionPool;
import com.weidian.proxy.hbase.common.exception.NoConnectionException;
import com.weidian.proxy.hbase.entity.RowKeyEntity;
import com.weidian.proxy.hbase.entity.TestEntity;
import com.weidian.proxy.hbase.spring.SpringHbaseDao;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jiang on 17/10/28.
 */
public class NativeHbaseDao<T> extends BaseHbaseDao<T>{
    private static final Logger LOGGER = LoggerFactory.getLogger(SpringHbaseDao.class);

    private volatile static HbaseConnectionPool hbaseConnectionPool = null;
    private Configuration configuration = null;
    private static Object lock = new Object();

    public NativeHbaseDao(Configuration configuration) {
        synchronized (lock) {
            if(hbaseConnectionPool == null) {
                hbaseConnectionPool = new HbaseConnectionPool();
            }
        }
        try {
            this.configuration = configuration;
            initConn();
        } catch (IOException e) {
            LOGGER.error("初始化hbase连接出错", e);
        } catch (InterruptedException e) {
            LOGGER.error("初始化hbase连接出错", e);
        }

    }

    public NativeHbaseDao(Configuration configuration,int poolSize,int maxPoolSize) {
        synchronized (lock) {
            if(hbaseConnectionPool == null) {
                hbaseConnectionPool = new HbaseConnectionPool(poolSize,maxPoolSize);
            }
        }

        try {
            this.configuration = configuration;
            initConn();
        } catch (IOException e) {
            LOGGER.error("初始化hbase连接出错",e);
        } catch (InterruptedException e) {
            LOGGER.error("初始化hbase连接出错",e);
        }
    }

    /**
     * hbase get data by rowkeys
     * @param clazz
     * @param rowkeys
     * @param tableName
     * @return
     * @throws Exception
     */
    public List<T> getDataByRowkeys (final Class clazz,
                                     final List<RowKeyEntity> rowkeys,
                                     final String tableName) throws Exception{


        Connection connection = getConnection();
        TableName tableName1 = TableName.valueOf(tableName);
        Table table = null;
        try {
            table = connection.getTable(tableName1);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            enqueueConnection(connection);
        }
        if(table == null) {
            LOGGER.error("can't get table");
            throw new NoConnectionException("can't get table");
        }
        List<T> objects = new ArrayList<T>(rowkeys.size());
        try {
            for (RowKeyEntity rowKeyEntity : rowkeys) {
                String rowkey = rowKeyEntity.getRowkey();
                Get get = new Get(Bytes.toBytes(rowkey));
                Result result = table.get(get);
                if (result == null || result.listCells() == null) {
                    LOGGER.error("not found data：" + rowkey);
                    continue;
                }
                appendObject(rowkey,result,clazz,objects);

            }
        } catch (Exception e) {
            LOGGER.error("query hbase data error : " + rowkeys.toString());
        } finally {

            table.close();
        }

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
        return scanDataByRowkeysWithFilters(clazz, tableN, maxRowKey, prefixRowkey, limit,null, isDesc);
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
            throw new IllegalArgumentException("prefix rowkey must not empty with desc is true");
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
     * scan的分页查询
     * @param clazz
     * @param tableN
     * @param maxRowKey
     * @param limit
     * @return
     */
    public List<T> scanDataByRowkeysWithFilters(final Class clazz,
                                     final String tableN,
                                     final String maxRowKey,
                                                final String prefixRowkey,
                                                final Long limit,List<Filter> outFilters,
                                                boolean isDesc) throws Exception{
        if(isDesc && StringUtils.isBlank(prefixRowkey)) {
            throw new IllegalArgumentException("prefix rowkey must not empty with desc is true");
        }

        List<T> objects = new ArrayList<T>();
        Connection connection = getConnection();
        TableName tableName = TableName.valueOf(tableN);
        Table table = connection.getTable(tableName);
        List<Filter> filters = new ArrayList<>(2);
        if(limit > 0) {
            filters.add(new PageFilter(limit));
        }
        if(outFilters != null && outFilters.size() > 0) {
            filters.addAll(outFilters);
        }
        if(StringUtils.isNotBlank(prefixRowkey)) {
            filters.add(new PrefixFilter(prefixRowkey.getBytes()));
        }
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        Scan s = new Scan();

        if(isDesc) {
            s.setStopRow(Bytes.toBytes(maxRowKey));
        } else {
            s.setStartRow(Bytes.toBytes(maxRowKey));
        }
        s.setFilter(list);
        ResultScanner rs = table.getScanner(s);

        parseHbaseResult(rs,objects,prefixRowkey,clazz);

        rs.close();

        return objects;
    }

    /**
     * 写hbase数据
     * @param tableN
     * @param cf
     * @param col
     * @param key
     * @param value
     * @throws Exception
     */
    public void putData(String tableN,String cf,String col,String key, Object value) throws Exception{
        Connection connection = getConnection();
        TableName tableName = TableName.valueOf(tableN);
        Table table = connection.getTable(tableName);

        enqueueConnection(connection);

        Put put = new Put(Bytes.toBytes(key));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(col),Bytes.toBytes(value.toString()));
        table.put(put);
        table.close();
    }

    /**
     * get a connection from queue
     * @return
     * @throws Exception
     */
    public Connection getConnection() throws Exception{
        Connection connection = hbaseConnectionPool.getConnection();
        if(connection == null) {
            try {
                boolean flag = createConnection();
                if(flag) {
                    connection = hbaseConnectionPool.getConnection();
                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        if(connection == null) {
            LOGGER.error("no more connection can get..... ");
            throw new NoConnectionException("no more connection can get");
        }
        return connection;
    }

    private void enqueueConnection(Connection connection) throws InterruptedException {
        if(connection != null) {
            hbaseConnectionPool.queueConn(connection);
        }
    }

    private void parseHbaseResult(ResultScanner rs, List<T> objects, String startRowKey, Class clazz) {
        for(Result result : rs) {
            String rowkey = Bytes.toString(result.getRow());
            if(StringUtils.isNotBlank(startRowKey) && !rowkey.startsWith(startRowKey)) {
                continue;
            }
            appendObject(rowkey,result,clazz,objects);
        }
    }

    private boolean appendObject(String rowkey,Result result,Class clazz,List<T> objects) {
        if (result == null || result.listCells() == null) {
            LOGGER.error("not found data：" + rowkey);
            return false;
        }

        Object object = convertToEntity(rowkey,clazz,result);
        if(object == null) {
            LOGGER.error("can't new instance object: " + clazz);
            return false;
        }

        objects.add((T)object);

        return true;
    }

    private void initConn() throws IOException,InterruptedException{
        int size = hbaseConnectionPool.getPoolSize();
        for(int i = 0; i < size; i++) {
            createConnection();
        }
    }

    /**
     * create a connection and inqueue
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private synchronized boolean createConnection() throws IOException,InterruptedException{
        Connection connection = ConnectionFactory.createConnection(configuration);
        boolean flag = hbaseConnectionPool.queueConn(connection);
        return flag;
    }



    public static void main(String[] args) throws Exception{

        Configuration configuration = HBaseConfiguration.create();
        if(args.length == 0) {
            configuration.set("hbase.zookeeper.quorum", "10.8.97.167");
            configuration.set("hbase.master", "10.8.97.167");
            configuration.set("hbase.zookeeper.property.clientPort", "2182");
        } else {
            configuration.set("hbase.zookeeper.quorum", args[0]);
            configuration.set("hbase.master", args[1]);
            configuration.set("hbase.zookeeper.property.clientPort", args[2]);
        }
        NativeHbaseDao nativeHbaseDao = new NativeHbaseDao(configuration,1,1);


//       nativeHbaseDao.putData("mesa:ods_buyer_bhv_info_n","cf","JSON","158374538_982706362_9223370531724167803","{\"sellerId\":\"835473851\",\"feedTime\":\"1505130608000\",\"feedType\":\"1020\",\"description\":\"\\xE5\\xB7\\xB2\\xE6\\x94\\xAF\\xE4\\xBB\\x98\",\"from\":\"1\",\"buyerId\":\"982706362\"}");
//       nativeHbaseDao.putData("mesa:ods_buyer_bhv_info_n","cf","actionType","158374538_982706362_9223370531724167803","103");
//       nativeHbaseDao.putData("mesa:ods_buyer_bhv_info_n","cf","JSON","158374538_982706362_9223370533813003806","{\"sellerId\":\"835473851\",\"feedTime\":\"1505130608000\",\"feedType\":\"1020\",\"description\":\"\\xE5\\xB7\\xB2\\xE6\\x94\\xAF\\xE4\\xBB\\x98\",\"from\":\"1\",\"buyerId\":\"982706362\"}");
//       nativeHbaseDao.putData("mesa:ods_buyer_bhv_info_n","cf","actionType","158374538_982706362_9223370533813003806","106");
//       nativeHbaseDao.putData("mesa:ods_buyer_bhv_info_n","cf","JSON","158374538_982706362_9223370533950204808","{\"sellerId\":\"835473851\",\"feedTime\":\"1505130608000\",\"feedType\":\"1020\",\"description\":\"\\xE5\\xB7\\xB2\\xE6\\x94\\xAF\\xE4\\xBB\\x98\",\"from\":\"1\",\"buyerId\":\"982706362\"}");
//       nativeHbaseDao.putData("mesa:ods_buyer_bhv_info_n","cf","actioinType","158374538_982706362_9223370533950204808","108");

        for(int i = 0; i < 1; i++) {
            new Thread(new TestThread(nativeHbaseDao,i)).start();
        }

//        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        for(int i = 0; i < 1; i++) {
//            executorService.execute(new Runnable() {
//                @Override
//                public void run() {
//                    System.out.println(Thread.currentThread().getName() + " " + new Date(0L));
//                }
//            });
//        }
    }

}

class TestThread implements Runnable {

    private NativeHbaseDao nativeHbaseDao;
    private int i;
    public TestThread(NativeHbaseDao nativeHbaseDao,int i) {
        this.nativeHbaseDao = nativeHbaseDao;
        this.i = i;
    }

    @Override
    public void run() {
        List<RowKeyEntity> rowKeyEntities = new ArrayList<RowKeyEntity>();
        RowKeyEntity rowKeyEntity = new RowKeyEntity();
        rowKeyEntity.setRowkey("0675420211_2198291473_1188518066");
        rowKeyEntity.setCf("cf");
        rowKeyEntities.add(rowKeyEntity);
        List<TestEntity> testEntities = null;
        try {
//            Filter filter = nativeHbaseDao.getOutFilter(SingleColumnValueFilter.class,"cf",
//                    "actionType", CompareFilter.CompareOp.EQUAL,
//                    "103");
//            SingleColumnValueFilter singleColumnValueFilter = null;
//            if(filter != null) {
//                singleColumnValueFilter = (SingleColumnValueFilter)filter;
//            }
//            List<Filter> filters = new ArrayList<Filter>();
//            filters.add(singleColumnValueFilter);
            testEntities = nativeHbaseDao.scanDataByAscRowkeys(TestEntity.class,"mesa:ods_buyer_bhv_info_n", "994096987_999332656_9223370522686600673","994096987_999332656",2L);
            System.out.println(i+"  "+testEntities.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
