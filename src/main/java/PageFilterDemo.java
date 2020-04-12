import com.lijie.coprocessor.HbaseUtil1;
import com.lijie.test.HbaseUtil1;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author lijie
 * 分页显示数据
 * 每页显示一条
 * 循环结束条件
 * <p>
 * firstkeyonlyfilter
 * firstkeyonlyfilter/1,得到总页数
 */
public class PageFilterDemo {
    public static void main(String[] args) throws IOException {

        int count = 0;

        PageFilter pageFilter = new PageFilter(2);
        Scan scan = new Scan();
        scan.setFilter(pageFilter);

        Connection connection = HbaseUtil1.getConnection();
        Table table = connection.getTable(TableName.valueOf("lixiaojie:emp"));
        String recoder = "";
        while (true) {
            //让row到下一行
            scan.setStartRow(Bytes.toBytes(recoder + "\001"));
            ResultScanner scanner = table.getScanner(scan);
            count = 0;
            for (Result result : scanner) {
                //计数器，保证最后一页可以跳出死循环
                count++;
                //每便利一行记录一下当前行
                recoder = Bytes.toString(result.getRow());
                //输出每一行的值
                for (Cell cell : result.rawCells()) {
                    System.out.print("\t" + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.print("\t" + Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.print("\t" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("\t" + Bytes.toString(CellUtil.cloneValue(cell)));
                }


            }

            if (count < 2) {
                break;
            }

            System.out.println("**********************************************************************************");
        }

    }
}
