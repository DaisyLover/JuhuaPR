package job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Ziyu and Brada on 2/6/14.
 * Parses wiki pages to find page title and wiki links
 */
public class AdjacencyOutGraphMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] structure = value.toString().split("\\t");
        if(structure.length <= 1 || structure[1].isEmpty()){
            context.write(new Text(structure[0]), new Text("#"));
        }
        for(int i = 1; i < structure.length; ++i){
            context.write(new Text(structure[i]), new Text(structure[0]));
        }
    }

   /* @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        String[] structure = value.toString().split("\\t");
        if(structure.length <= 1 || structure[1].isEmpty()){
            outputCollector.collect(new Text(structure[0]), new Text("#"));
        }
        for(int i = 1; i < structure.length; ++i){
            outputCollector.collect(new Text(structure[i]), new Text(structure[0]));
        }
    }*/
}
