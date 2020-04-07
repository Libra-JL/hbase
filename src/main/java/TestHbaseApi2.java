import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;


public class TestHbaseApi2 {
    public static void main(String[] args) throws IOException {
        Admin admin = HbaseUtil.getAdmin();
        TableName student = TableName.valueOf("lixiaojie:emp");
        if (admin.tableExists(student)){
            Table table = HbaseUtil.getTable(student);
//            admin.disableTable(student);
//            admin.deleteTable(student);
//            System.out.println("表删除了");
//            Delete delete = new Delete();
//            table.delete();
//            Delete delete = new Delete(Bytes.toBytes("1001"));
//            table.delete(delete);
//            System.out.println("删除成功");

            //扫描数据
            Table table1 = HbaseUtil.getTable(student);
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.print("family"+ Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.print("\trowkey"+ Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.print("\tcolumn"+ Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("\tvalue"+ Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }


        }




    }
}
