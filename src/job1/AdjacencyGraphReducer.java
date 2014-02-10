package job1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Ziyu on 2/6/14.
 */
public class AdjacencyGraphReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder inlinkGraph = new StringBuilder();
        boolean hasPound = false;
        while(values.iterator().hasNext()){
            String tmp = values.iterator().next().toString();
            if("#".equals(tmp)){
                hasPound = true;
            }
            else{
                inlinkGraph.append('\t');
                inlinkGraph.append(tmp);
            }
        }
        if(hasPound){
            context.write(key, new Text(inlinkGraph.toString().trim()));
        }
    }

/*    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        StringBuilder inlinkGraph = new StringBuilder();
        boolean hasPound = false;
//        System.out.println("Reducing key: " + key.toString());
        while(values.hasNext()){
            String tmp = values.next().toString();
            if("#".equals(tmp)){
                hasPound = true;
            }
            else{
                inlinkGraph.append('\t');
                inlinkGraph.append(tmp);
            }
        }
//        System.out.println("Reduced output: " + inlinkGraph.toString());
        if(hasPound){
            outputCollector.collect(key, new Text(inlinkGraph.toString().trim()));
        }
    }*/
}
