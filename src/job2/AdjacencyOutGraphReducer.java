package job2;

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
public class AdjacencyOutGraphReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        StringBuilder outlinkGraph = new StringBuilder();
        outlinkGraph.append("1.00");
//        System.out.println("Reducing key: " + key.toString());
        while(values.hasNext()){
            String tmp = values.next().toString();
            if("#".equals(tmp)) continue;
            if(!tmp.isEmpty()){
                outlinkGraph.append('\t');
                outlinkGraph.append(tmp);
            }
        }
        outputCollector.collect(key, new Text(outlinkGraph.toString()));
    }
}
