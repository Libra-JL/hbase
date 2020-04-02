import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class FilterListDemo {
    public static void main(String[] args) throws IOException {
        //创建filterlist
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //构造过滤条件
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes("info"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.LESS,
                Bytes.toBytes("60")
        );

        SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter(
                Bytes.toBytes("baseinfo"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("gaogao")
        );
        singleColumnValueFilter.setFilterIfMissing(true);
        singleColumnValueFilter1.setFilterIfMissing(true);
        //加入过滤连
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(singleColumnValueFilter1);

        Scan scan = new Scan();
        scan.setFilter(filterList);


        //获取表对象
        TableName tableName = TableName.valueOf("default:test");
        Table table = HbaseUtil.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));

            }
        }
    }
}
