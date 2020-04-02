
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.Test;

import java.io.IOException;

/**
 * @author lijie
 */
public class TbDDL {

    @Test
    public void existsTable() throws IOException {
        Admin admin = HbaseUtil.getAdmin();
        TableName student = TableName.valueOf("student");
        boolean b = admin.tableExists(student);
        System.out.println(b);

    }
/*
    @Test
    public void describeTable(){
        Admin admin = HbaseUtil.getAdmin();
        HTableDescriptor tableDescriptor = admin.getTableDescriptor();
        if (tableDescriptor==null){
            return;
        }
        System.out.println(tableDescriptor.toString());
    }*/
/*

    @Test
    public void deleteFamily(){
        Admin admin = HbaseUtil.getAdmin();
        TableName tableName = TableName.valueOf("");
        if (!admin.tableExists(tableName)){
            admin.deleteColumn();
        }
    }
*/

}
