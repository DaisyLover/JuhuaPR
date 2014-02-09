package job5;

import Common.PageRankedWikiPage;
import Common.Parameters;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Ziyu on 2/6/14.
 */
public class PageRankOrderingMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
//    @Override
//    public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> collector, Reporter reporter) throws IOException {
//        double threshold =
//        PageRankedWikiPage page = new PageRankedWikiPage();
//        page.restoreFromString(value.toString());
//        if(page.getPageRank() >= )
//        collector.collect(new DoubleWritable(page.getPageRank()), new Text(page.getTitle()));
//    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        double n = Double.parseDouble(context.getConfiguration().get(Parameters.N_KEY));
        double threshold = 5.0;
        PageRankedWikiPage page = new PageRankedWikiPage();
        page.restoreFromString(value.toString());
        if(page.getPageRank() >= threshold)
            context.write(new DoubleWritable(page.getPageRank() / n), new Text(page.getTitle()));
    }
}
