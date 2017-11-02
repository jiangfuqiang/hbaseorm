package com.weidian.proxy.hbase.common;

import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jiang on 17/10/28.
 */
public class HbaseConnectionPool {

    private volatile BlockingQueue<Connection> connectionPool = null;
    private static final int default_size = 10;
    private static int maxSize = default_size;
    private static int poolSize = default_size;
    private static int maxWaitTime = 300;
    //连接计数器
    private AtomicInteger atomicInteger = new AtomicInteger(0);

    public HbaseConnectionPool() {
        this(poolSize,maxSize,maxWaitTime);
    }

    public HbaseConnectionPool(int poolSize,int maxSize) {
        this(poolSize,maxSize,maxWaitTime);
    }

    public HbaseConnectionPool(int poolSize,int maxSize,int maxWaitTime) {
        connectionPool = new ArrayBlockingQueue<Connection>(poolSize);
        this.maxSize = maxSize;
        this.poolSize = poolSize;
        this.maxWaitTime = maxWaitTime;
    }

    /**
     * 获取一个连接
     * @return
     */
    public Connection getConnection() throws InterruptedException{
        long time = System.currentTimeMillis() + maxWaitTime;
        Connection connection = connectionPool.poll(time, TimeUnit.SECONDS);
        atomicInteger.decrementAndGet();
        return connection;
    }

    /**
     * 连接存入队列
     * @param connection
     * @return
     * @throws InterruptedException
     */
    public boolean queueConn(Connection connection) throws InterruptedException {
        if(atomicInteger.get() >= maxSize) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return  false;
        }
        connectionPool.put(connection);
        atomicInteger.incrementAndGet();
        return true;
    }

    public void closeConnection(Connection connection) {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }
}
