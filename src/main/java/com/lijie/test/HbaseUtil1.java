package com.lijie.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtil1 {
    private HbaseUtil1(){}

    private static ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();


    /**
     * 获取hbase连接
    * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {

        Connection connection = connHolder.get();
        if (connection==null){
            Configuration entries = HBaseConfiguration.create();
            entries.set("hbase.zookeeper.quorum", "192.168.5.101:2181,192.168.5.102:2181,192.168.5.103:2181");
            entries.set("hbase.zookeeper.property.clientPort", "2181");
            Connection conn = ConnectionFactory.createConnection(entries);
            connHolder.set(conn);
            return conn;
        }else {
            return connHolder.get();
        }
    }


    public static void insertData(String tableName,String rowKey,String family,String column,String value) throws IOException {
        Connection connection = connHolder.get();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);

    }


    /**
     * 关闭
     * @throws IOException
     */
    public static void close() throws IOException {
        Connection connection = connHolder.get();
        if (connection!=null){
            connection.close();
            connHolder.remove();
        }
    }


}
