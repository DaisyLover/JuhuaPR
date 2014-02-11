package job4;

import Common.PageRankedWikiPage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Ziyu on 2/6/14.
 */
public class PageRankCalculationMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        PageRankedWikiPage page = new PageRankedWikiPage();
        page.restoreFromString(value.toString());
        //send graph info to self
        context.write(new Text(page.getTitle()), value);
        //calculate page rank to distribute
        if(!page.getOutLinks().isEmpty())
        {
            Double pageRank = page.getPageRank() / page.getOutLinks().size();
            Text pageRankText = new Text(Double.toString(pageRank));
            for(String outlink: page.getOutLinks()){
                context.write(new Text(outlink), pageRankText);
            }
        }
    }

/*    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        PageRankedWikiPage page = new PageRankedWikiPage();
        page.restoreFromString(value.toString());
        //send graph info to self
        collector.collect(new Text(page.getTitle()), value);
        //calculate page rank to distribute
        if(!page.getOutLinks().isEmpty())
        {
            Double pageRank = page.getPageRank() / page.getOutLinks().size();
            Text pageRankText = new Text(Double.toString(pageRank));
//            System.out.printf("Mapper: distributing page rank of %s with value=%s\n", page.getTitle(), pageRankText.toString());
            for(String outlink: page.getOutLinks()){
                collector.collect(new Text(outlink), pageRankText);
            }
        }
    }*/
}
