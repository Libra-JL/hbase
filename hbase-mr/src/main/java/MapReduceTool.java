import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class MapReduceTool implements Tool {
    public int run(String[] args) throws Exception {
        Job instance = Job.getInstance();
        instance.setJarByClass(MapReduceTool.class);

        TableMapReduceUtil.initTableMapperJob("lixiaojie:emp", new Scan(), ScanDataMapper.class, ImmutableBytesWritable.class, Put.class, instance);
        TableMapReduceUtil.initTableReducerJob("lixiaojie:users", InsertDataReducer.class, instance);

        boolean b = instance.waitForCompletion(true);

        return b? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }
}
