package PageRank.job4;

import PageRank.Common.PageRankedWikiPage;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Ziyu on 2/6/14.
 */
public class PageRankCalculationReducer extends Reducer<Text, Text, Text, Text> {
    static final double DAMPING_FACTOR = 0.85;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double pageRank = 0.0;
        PageRankedWikiPage page = new PageRankedWikiPage();
        while (values.iterator().hasNext()){
            String tmp = values.iterator().next().toString();
            if(tmp.contains("\t")){
                //restore node structure
                page.restoreFromString(tmp);
//                System.out.println("Received structure info: " + tmp);
//                System.out.println("Page restored as: " + page.serializeGrapnInfo());
            } else{
                pageRank += Double.parseDouble(tmp);
//                System.out.printf("Reducer %s received page rank = %s\N", key.toString(), tmp);
            }
        }
//        System.out.printf("Reducer %s received total page rank = %s\N", key.toString(), Double.toString(pageRank));
        page.setPageRank((1.0 - DAMPING_FACTOR) + DAMPING_FACTOR * pageRank);
        context.write(key, new Text(page.serializeGrapnInfo()));
    }

    /*@Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        double pageRank = 0.0;
        PageRankedWikiPage page = new PageRankedWikiPage();
        while (values.hasNext()){
            String tmp = values.next().toString();
            if(tmp.contains("\t")){
                //restore node structure
                page.restoreFromString(tmp);
//                System.out.println("Received structure info: " + tmp);
//                System.out.println("Page restored as: " + page.serializeGrapnInfo());
            } else{
                pageRank += Double.parseDouble(tmp);
//                System.out.printf("Reducer %s received page rank = %s\N", key.toString(), tmp);
            }
        }
//        System.out.printf("Reducer %s received total page rank = %s\N", key.toString(), Double.toString(pageRank));
        page.setPageRank((1.0 - DAMPING_FACTOR) + DAMPING_FACTOR * pageRank);
        collector.collect(key, new Text(page.serializeGrapnInfo()));
    }*/
}
