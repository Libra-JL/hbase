import org.apache.hadoop.util.ToolRunner;
import tool.hbase2hdfsTool;


public class HdfsToHdfs {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new hbase2hdfsTool(), args);
    }
}
