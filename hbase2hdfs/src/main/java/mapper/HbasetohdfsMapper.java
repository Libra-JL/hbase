package mapper;

import com.google.common.base.Strings;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sun.security.util.Length;

import java.io.IOException;

public class HbasetohdfsMapper extends TableMapper<Text, NullWritable> {
    Text k = new Text();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        for (Cell cell : value.rawCells()) {
            stringBuffer.append(Bytes.toString(CellUtil.cloneQualifier(cell))+":");
            stringBuffer.append(Bytes.toString(CellUtil.cloneValue(cell))+"\t");
        }
        k.set(stringBuffer.toString().substring(0, stringBuffer.length()-1));
        context.write(k, NullWritable.get());
    }
}
