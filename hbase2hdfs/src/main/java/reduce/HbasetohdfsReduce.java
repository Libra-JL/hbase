package reduce;

import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class HbasetohdfsReduce extends TableReducer<> {
    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {

        context.write(key, NullWritable.get());

    }
}
