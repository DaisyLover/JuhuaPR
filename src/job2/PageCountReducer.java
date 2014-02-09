package job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Ziyu on 2/8/14.
 */
public class PageCountReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        long count = 0;
        while (values.hasNext()){
            count++;
            values.next();
        }
        collector.collect(key, new Text(Long.toString(count)));
    }
}
