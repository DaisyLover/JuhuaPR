package job4;

import Common.PageRankedWikiPage;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Ziyu on 2/6/14.
 */
public class PageRankCalculationReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    static final double DAMPING_FACTOR = 0.85;

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        double pageRank = 0.0;
        PageRankedWikiPage page = new PageRankedWikiPage();
        while (values.hasNext()){
            String tmp = values.next().toString();
            if(tmp.contains("\t")){
                //restore node structure
                page.restoreFromString(tmp);
            } else{
                pageRank += Double.parseDouble(tmp);
            }
        }
        page.setPageRank((1.0 - DAMPING_FACTOR) * page.getPageRank() + DAMPING_FACTOR * pageRank);
        collector.collect(key, new Text(page.serializeGrapnInfo()));
    }
}
