
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

}
