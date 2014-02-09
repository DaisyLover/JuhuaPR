import Common.Parameters;
import job1.AdjacencyGraphMapper;
import job1.AdjacencyGraphReducer;
import job1.XmlInputFormat;
import job2.PageCountMapper;
import job2.PageCountReducer;
import job3.PageRankCalculationMapper;
import job3.PageRankCalculationReducer;
import job4.PageRankOrderingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * Created by ziyu on 2/6/14.
 */
public class PageRank {

    public static void main(String[] args) throws Exception {
        PageRankMaster master = new PageRankMaster();

        master.buildAdjacencyGraph("/usr/hduser/pr/enwiki-latest-pages-articles1.xml", "/usr/hduser/pr/PageRank.inlink.out");
//        master.countPages();


        for (int runs = 0; runs < 5; runs++) {
            master.calcuatePageRank(String.format("results/PageRank.iter%d.out", runs), String.format("results/PageRank.iter%d.out", runs + 1));
        }
    }


}

class PageRankMaster{
    private long n = 0;

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

    public void countPages(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(PageRankMaster.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setMapperClass(PageCountMapper.class);
        conf.setReducerClass(PageCountReducer.class);

        JobClient.runJob(conf);
    }

    public void calcuatePageRank(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(PageRankMaster.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setMapperClass(PageRankCalculationMapper.class);
        conf.setReducerClass(PageRankCalculationReducer.class);

        JobClient.runJob(conf);
    }

    public void orderResultByPageRank(String inputPath, String outputPath, String nDataPath) throws IOException, ClassNotFoundException, InterruptedException {
        Path pt=new Path(nDataPath);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line = br.readLine();
        n = Long.parseLong(line.split("\\t")[1]);

        Path input = new Path(inputPath);
        Path output = new Path(outputPath);
        Path tempDirPath = new Path(output, "tmp");

        // Create the job configuration
        Configuration conf = new Configuration();

        // Pass in our custom options.
        conf.set(Parameters.N_KEY, Double.toString(n));

        // get the FileSystem instances for each path
        // this allows for the paths to live on different FileSystems (local, hdfs, s3, etc)
        FileSystem inputFS = input.getFileSystem(conf);
        FileSystem outputFS = tempDirPath.getFileSystem(conf);

        // if input path does not exists, fail
        if (!inputFS.exists(input)) {
            System.out.println("Input file does not exist: " + input);
            System.exit(-1);
        }

        // if output path exists, delete recursively
        if (outputFS.exists(tempDirPath)) {
            outputFS.delete(tempDirPath, true);
        }

        // Create the actual job and run it.
        Job job = new Job(conf, "ordering page rank output job");
        // finds the enclosing jar path
        job.setJarByClass(PageRankMaster.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        org.apache.hadoop.mapreduce.lib.input.TextInputFormat.setInputPaths(job, input);

        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setOutputPath(job, tempDirPath);

        // our mapper class
        job.setMapperClass(PageRankOrderingMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        // run job and block until job is done, printing progress
        job.waitForCompletion(true);
    }
}
