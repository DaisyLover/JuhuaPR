package PageRank.job3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by Ziyu on 2/8/14.
 */
public class PageCountReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        while (values.iterator().hasNext()){
            count++;
            values.iterator().next();
        }
        context.write(new Text("N=" + count), new Text(""));
    }

   /* @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        long count = 0;
        while (values.hasNext()){
            count++;
            values.next();
        }
        collector.collect(new Text("N="+count), new Text(""));
    }*/
}
