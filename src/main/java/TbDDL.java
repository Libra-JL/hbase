import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.BloomType;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TbDDL {
    private static Admin admin = null;
    private static TableName tableName = TableName.valueOf("test");

    @Before
    public void init() {
        admin = HbaseUtil.getAdmin();
    }

    /**
     * 创建表
     */
    @Test
    public void createTable() throws IOException {
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        //创建列族描述器
        HColumnDescriptor info = new HColumnDescriptor(Bytes.toBytes("info"));
        info.setBloomFilterType(BloomType.ROW);
        info.setVersions(1, 3);
        info.setMaxVersions(3);
        info.setMinVersions(1);
        hTableDescriptor.addFamily(info);
        admin.createTable(hTableDescriptor);

    }


    @After
    public void close() {
        HbaseUtil.closeAdmin(admin);
    }
}
