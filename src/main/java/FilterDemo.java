import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
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
                System.out.print("\t"+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.print("\t"+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.print("\t"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("\t"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        HbaseUtil1.close();


    }
}
