import Common.Parameters;
import job1.AdjacencyGraphMapper;
import job1.AdjacencyGraphReducer;
import job1.XmlInputFormat;
import job2.AdjacencyOutGraphMapper;
import job2.AdjacencyOutGraphReducer;
import job3.PageCountMapper;
import job3.PageCountReducer;
import job4.PageRankCalculationMapper;
import job4.PageRankCalculationReducer;
import job5.PageRankOrderingMapper;
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
        String bucketName = args[0];
        master.buildInGraph(bucketName + "/results/enwiki-latest-pages-articles1.xml", bucketName + "/results/PageRank.inlink.out");
        master.buildOutGraph(bucketName + "/results/PageRank.inlink.out", bucketName + "/results/PageRank.outlink.out");
        master.countPages(bucketName + "/results/PageRank.outlink.out", bucketName + "/results/PageRank.n.out");

        master.calcuatePageRank(bucketName + "/results/PageRank.outlink.out", bucketName + "/tmp/PageRank.iter1.out");
        for (int runs = 1; runs < 8; runs++) {
            master.calcuatePageRank(String.format("%s/tmp/PageRank.iter%d.out", bucketName, runs), String.format("%s/tmp/PageRank.iter%d.out", bucketName, runs + 1));
        }
        master.orderResultByPageRank(bucketName + "/tmp/PageRank.iter1.out", bucketName + "results/PageRank.iter1.out", bucketName + "/results/PageRank.n.out");
        master.orderResultByPageRank(bucketName + "/tmp/PageRank.iter8.out", bucketName + "results/PageRank.iter8.out", bucketName + "/results/PageRank.n.out");
    }


}

class PageRankMaster{
    private long n = 0;

    public void buildInGraph(String inputPath, String outputPath) throws IOException {
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

    public void buildOutGraph(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(PageRankMaster.class);

        // Setup input for mapper
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        conf.setInputFormat(TextInputFormat.class);
        conf.setMapperClass(AdjacencyOutGraphMapper.class);

        // Setup output from reducer
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setReducerClass(AdjacencyOutGraphReducer.class);

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
