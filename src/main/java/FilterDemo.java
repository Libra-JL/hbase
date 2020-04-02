import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class FilterDemo {
    @Test
    public void familyFilter() throws IOException {
        ByteArrayComparable regexStringComparator = new RegexStringComparator("^info");
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, regexStringComparator);

        Connection connection = HbaseUtil1.getConnection();
        TableName tableName = TableName.valueOf("lixiaojie:emp");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        scan.setFilter(familyFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print("\t" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.print("\t" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.print("\t" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("\t" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        HbaseUtil1.close();


    }


    @Test
    public void MultiColumnPrefix() throws IOException {
        byte[][] bytes = {
                Bytes.toBytes("age"), Bytes.toBytes("age")
        };
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(bytes);
        Connection connection = HbaseUtil1.getConnection();
        TableName tableName = TableName.valueOf("lixiaojie:emp");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan().setFilter(multipleColumnPrefixFilter);
        table.getScanner(scan);
    }


    @Test
    public void rowkeyfilter() throws IOException {
        RegexStringComparator regexStringComparator = new RegexStringComparator("^10");
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, regexStringComparator);

        Connection connection = HbaseUtil1.getConnection();
        Table table = connection.getTable(TableName.valueOf("lixiaojie:emp"));
        Scan scan = new Scan().setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print("\t" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.print("\t" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.print("\t" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("\t" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        HbaseUtil1.close();
    }


    @Test
    public void firstkeyonlyfilterdemo() throws IOException {
        //主要用在计数
        //取出改行的第一列
        FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();
        Connection connection = HbaseUtil1.getConnection();
        Table table = connection.getTable(TableName.valueOf("lixiaojie:emp"));
        Scan scan = new Scan().setFilter(firstKeyOnlyFilter);
        ResultScanner scanner = table.getScanner(scan);
//        scanner.iterator().hasNext()
        int count = 0;
        for (Result result : scanner) {
            count++;
        }
        System.out.println(count);
        HbaseUtil1.close();

    }
}
