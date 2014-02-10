package job2;

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
public class AdjacencyOutGraphMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        String[] structure = value.toString().split("\\t");
        if(structure.length <= 1 || structure[1].isEmpty()){
            outputCollector.collect(new Text(structure[0]), new Text("#"));
        }
        for(int i = 1; i < structure.length; ++i){
            outputCollector.collect(new Text(structure[i]), new Text(structure[0]));
        }
    }
}
