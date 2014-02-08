import job1.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by ziyu on 2/6/14.
 */
public class PageRank {

    public static void main(String[] args) throws Exception {
        PageRankMaster master = new PageRankMaster();

        master.buildAdjacencyGraph("/usr/hduser/pr/enwiki-latest-pages-articles1.xml", "/usr/hduser/pr/PageRank.inlink.out");
    }


}

class PageRankMaster{
    public void buildAdjacencyGraph(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(PageRankMaster.class);

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        // Setup input for mapper
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        conf.setInputFormat(XmlInputFormat.class);
        conf.setMapperClass(AdjacencyGraphMapper.class);

        // Setup output from reducer
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setReducerClass(AdjacencyGraphReducer.class);

        JobClient.runJob(conf);
    }
}
