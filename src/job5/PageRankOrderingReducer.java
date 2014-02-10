package job5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ziyu on 2/10/14.
 */
public class PageRankOrderingReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        while(values.iterator().hasNext()){
            context.write(values.iterator().next(), new Text(String.format("%.2f", key.get())));
        }
    }
}
