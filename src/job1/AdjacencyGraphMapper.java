package job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Ziyu and Brada on 2/6/14.
 * Parses wiki pages to find page title and wiki links
 */
public class AdjacencyGraphMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        XmlWikiPage wikiPage = new XmlWikiPage(value);
        wikiPage.parse();
        Text thisPage = wikiPage.getTitle();
        context.write(thisPage, new Text("#"));
        for(Text linkedPage: wikiPage.getLinks()){
            context.write(linkedPage, thisPage);
        }
    }

/*    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        XmlWikiPage wikiPage = new XmlWikiPage(value);
        wikiPage.parse();
        Text thisPage = wikiPage.getTitle();
//        System.out.println("Mapping Current page: " + thisPage.toString());
        outputCollector.collect(thisPage, new Text("#"));
        for(Text linkedPage: wikiPage.getLinks()){
//            System.out.println("Collected linked page: " + linkedPage.toString());
            outputCollector.collect(linkedPage, thisPage);
        }
    }*/
}
