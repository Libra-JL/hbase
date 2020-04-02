import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author lijie
 */
public class SingleColumnValueFilterDemo {
    public static void main(String[] args) throws IOException {

        RegexStringComparator regexStringComparator = new RegexStringComparator("^li");

        SubstringComparator li = new SubstringComparator("li");

        BinaryComparator lijie = new BinaryComparator(Bytes.toBytes("lijie2"));

        BinaryPrefixComparator lijie1 = new BinaryPrefixComparator(Bytes.toBytes("lijie"));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes("info"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
//                regexStringComparator
//                li
                lijie
//                lijie1
        );

        singleColumnValueFilter.setFilterIfMissing(true);

        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);

        TableName lixiaojie = TableName.valueOf("lixiaojie:emp");
        Connection connection = HbaseUtil1.getConnection();
        Table table = connection.getTable(lixiaojie);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}
