package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.naming.Name;
import javax.sound.sampled.Line;
import java.io.IOException;
import java.util.Iterator;

public class HdfsToHbase {
    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static Text k = new Text();
        private static LongWritable v = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");

            for (String s : split) {
                if (s.startsWith("age:")) {
                    String[] split1 = s.split(":");
                    k.set(split1[1]);
                    context.write(k, v);
                    break;
                }
            }
        }
    }


    //    static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
//
//    }
    static class MyReduce extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0L;
            Iterator<LongWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                LongWritable next = iterator.next();
                count += next.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("num"), Bytes.toBytes(count));

            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration entries = new Configuration();
        entries.set("hbase.zookeeper.quorum", "192.168.5.101,192.168.5.102,192.168.5.103");
        entries.set("fs.defaultFS", "hdfs://192.168.5.101:9000");
        Job instance = Job.getInstance(entries);

        instance.setJarByClass(HdfsToHbase.class);
        instance.setMapperClass(MyMapper.class);
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(LongWritable.class);


        TableMapReduceUtil.initTableReducerJob("lixiaojie:countname", MyReduce.class, instance);

        FileInputFormat.setInputPaths(instance, new Path(args[0]));
        instance.waitForCompletion(true);

    }

    private static void createtable(String s) throws IOException {
        Connection connection = HbaseUtil1.getConnection();
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(s);
        if (admin.tableExists(tableName)) {
            if (admin.isTableEnabled(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }else {
                admin.deleteTable(tableName);
            }
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
        //hColumnDescriptor.setBloomFilterType()
        hTableDescriptor.addFamily(hColumnDescriptor);

        admin.createTable(hTableDescriptor);
    }
}
