import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;


import java.io.IOException;

public final class HbaseUtil {
    private HbaseUtil(){}

    public static Configuration conf;
    private static Admin admin = null;
    private static Connection connection = null;

    public static Admin getAdmin(){

        //获取连接对象
        conf = HBaseConfiguration.create();
        //设置链接参数
        conf.set("hbase.zookeeper.quorum", "192.168.5.101:2181,192.168.5.102:2181,192.168.5.103:2181");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            return admin;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Table getTable(TableName tableName) throws IOException {
        return connection.getTable(tableName);
    }

    public static void closeAdmin(Admin admin){
        //判断参数是否为空
        if (admin==null){
            return;
        }
        try {
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
