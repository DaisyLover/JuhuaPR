package job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by Ziyu and Brada on 2/6/14.
 * Parses wiki pages to find page title and wiki links
 */
public class AdjacencyGraphMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> textTextOutputCollector, Reporter reporter) throws IOException {
        XmlWikiPage wikiPage = new XmlWikiPage(value);
        wikiPage.parse();
        Text thisPage = wikiPage.getTitle();
        for(Text linkedPage: wikiPage.getLinks()){
            textTextOutputCollector.collect(thisPage, linkedPage);
        }
    }
}
