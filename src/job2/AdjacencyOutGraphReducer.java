package job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Ziyu on 2/6/14.
 */
public class AdjacencyOutGraphReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder outlinkGraph = new StringBuilder();
        outlinkGraph.append("1.00");
//        System.out.println("Reducing key: " + key.toString());
        while(values.iterator().hasNext()){
            String tmp = values.iterator().next().toString();
            if("#".equals(tmp)) continue;
            if(!tmp.isEmpty()){
                outlinkGraph.append('\t');
                outlinkGraph.append(tmp);
            }
        }
        context.write(key, new Text(outlinkGraph.toString()));
    }

/*    @Override
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
    }*/
}
