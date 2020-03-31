import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * api连接namespace并
 * 创建namespace
 */
public class Demo1 {
    public static Configuration conf;

    static {
        //获取连接对象
        conf = HBaseConfiguration.create();
        //设置链接参数
        conf.set("hbase.zookeeper.quorum", "192.168.5.101:2181,192.168.5.102:2181,192.168.5.103:2181");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

    }

    public static void main(String[] args) throws IOException {
        //获取hbase连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取操作对象
        Admin connectionAdmin = connection.getAdmin();
        //创建一个namespace的描述器
        NamespaceDescriptor apiNamespace = NamespaceDescriptor.create("wocao").build();
        apiNamespace.setConfiguration("nz1908", "day day up");
        //创建namespace
        connectionAdmin.createNamespace(apiNamespace);
        //关闭资源
        connectionAdmin.close();
        connection.close();
    }
}
