import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestHbaseApi {
    public static void main(String[] args) throws IOException {
        Admin admin = HbaseUtil.getAdmin();
        try {
            admin.getNamespaceDescriptor("lixiaojie");
        } catch (NamespaceNotFoundException e) {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("lixiaojie").build();
            admin.createNamespace(namespaceDescriptor);
        }

        TableName tableName = TableName.valueOf("lixiaojie:emp");
        boolean b = admin.tableExists(tableName);
        if (b){
            //query data
            Table table = HbaseUtil.getTable(tableName);
            String rowkey = "1001";
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            boolean empty = result.isEmpty();
            if (empty){
                Put put = new Put(Bytes.toBytes(rowkey));
                String info = "info";
                String column = "name";
                String val = "lijie";
                put.addColumn(Bytes.toBytes(info), Bytes.toBytes(column),Bytes.toBytes(val));
                table.put(put);
            }else {
                for (Cell cell : result.rawCells()) {
//                    cell.
                    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println(CellUtil.cloneRow(cell));
                    System.out.println(CellUtil.cloneFamily(cell));
                    System.out.println(CellUtil.cloneQualifier(cell));
                }
            }
        }else {
            //create table
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor info = new HColumnDescriptor("info");
            hTableDescriptor.addFamily(info);
            admin.createTable(hTableDescriptor);
            System.out.println("表格创建成功");
        }



    }
}
