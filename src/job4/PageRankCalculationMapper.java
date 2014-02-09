package job4;

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
        String[] structure = value.toString().split("\t");
        //send graph info to self
        collector.collect(new Text(structure[0]), value);
        //calculate page rank to distribute
        Double pageRank = Double.parseDouble(structure[1]);
        pageRank /= structure.length - 2;
        Text pageRankText = new Text(pageRank.toString());
        for(int i = 2; i < structure.length; i++){
            collector.collect(new Text(structure[i]), pageRankText);
        }
    }
}
