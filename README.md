# hbaseorm
hbase的orm轻量级框架。
可以使用原生API也可以通过spring方式引入。只需一行代码解决hbase冗余的各种取值
1.spring配置：
 <!-- default HBase configuration -->
    <hdp:hbase-configuration delete-connection="false">
        hbase.master=${hbase.master}
        hbase.zookeeper.quorum=${hbase.zookeeper.quorum}
        hbase.zookeeper.property.clientPort=${hbase.zookeeper.property.clientPort}
    </hdp:hbase-configuration>

    <!-- wire hbase configuration (using default name 'hbaseConfiguration')
        into the template -->
    <bean id="hbaseTemplate" class="org.springframework.data.hadoop.hbase.HbaseTemplate">
        <property name="configuration" ref="hbaseConfiguration"/>
    </bean>

    <bean id="springHbaseDao" class="com.weidian.proxy.hbase.spring.SpringHbaseDao">
        <property name="hbaseTemplate" ref="hbaseTemplate"/>
    </bean>
    
    之后注解springHbaseDao即可
    
 2.native配置：
 Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","10.8.97.167");
        configuration.set("hbase.master","10.8.97.167");
        configuration.set("hbase.zookeeper.property.clientPort","2182");
        
 NativeHbaseDao nativeHbaseDao = new NativeHbaseDao(configuration,1,1);
