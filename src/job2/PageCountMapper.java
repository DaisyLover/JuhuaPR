package job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by Ziyu on 2/8/14.
 */
public class PageCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
    private Text identicalKey = new Text("N=");
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> collector, Reporter reporter) throws IOException {
        collector.collect(identicalKey, new LongWritable(1));
    }
}
