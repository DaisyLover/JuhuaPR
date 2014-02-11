package PageRank.job34;

import PageRank.Common.Parameters;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ziyu on 2/10/14.
 */
public class PageRankCalcInitMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        long n = context.getConfiguration().getLong(Parameters.N_KEY, 1);
        int start = value.find("\t");
        String title = Text.decode(value.getBytes(), 0, start);
        String links = Text.decode(value.getBytes(), start+1, value.getLength() - start - 1);
        context.write(new Text(title), new Text(1.0/n + "\t" + links));
    }
}
