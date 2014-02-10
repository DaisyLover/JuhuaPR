package job4;

import Common.PageRankedWikiPage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by Ziyu on 2/6/14.
 */
public class PageRankCalculationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
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
    }
}
