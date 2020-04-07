package tool;

import mapper.HbasetohdfsMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

import java.lang.annotation.Retention;

public class hbase2hdfsTool implements Tool {
    public int run(String[] args) throws Exception {
        Configuration entries = new Configuration();
        entries.set("hbase.zookeeper.quorum", "192.168.5.101,192.168.5.102,192.168.5.103");
        entries.set("fs.defaultFS", "hdfs://192.168.5.101:9000");
        Job instance = Job.getInstance(entries);
        instance.setJarByClass(hbase2hdfsTool.class);
        //还可以把scan提取出来一个方法加入过滤器再放到里面
        TableMapReduceUtil.initTableMapperJob("lixiaojie:emp", new Scan(), HbasetohdfsMapper.class, Text.class, NullWritable.class, instance);
        boolean b = instance.waitForCompletion(true);
        return b? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();

    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }
}
