import org.apache.hadoop.util.ToolRunner;

public class Table2Table {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MapReduceTool(), args);
    } 
}
