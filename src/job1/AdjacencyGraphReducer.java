package job1;

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
public class AdjacencyGraphReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text text, Iterator<Text> textIterator, OutputCollector<Text, Text> textTextOutputCollector, Reporter reporter) throws IOException {

    }
}
