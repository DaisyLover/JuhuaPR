package PageRank.job3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Ziyu on 2/8/14.
 */
public class PageCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text identicalKey = new Text("N=");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(identicalKey, new Text("1"));
    }

    /*@Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        collector.collect(identicalKey, new Text("1"));
    }*/
}
