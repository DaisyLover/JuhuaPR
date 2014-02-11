package PageRank;

import PageRank.Common.DescendingDoubleComparator;
import PageRank.Common.Parameters;
import PageRank.job1.AdjacencyGraphMapper;
import PageRank.job1.AdjacencyGraphReducer;
import PageRank.job1.XmlInputFormat;
import PageRank.job2.AdjacencyOutGraphMapper;
import PageRank.job2.AdjacencyOutGraphReducer;
import PageRank.job3.PageCountMapper;
import PageRank.job3.PageCountReducer;
import PageRank.job34.PageRankCalcInitMapper;
import PageRank.job4.PageRankCalculationMapper;
import PageRank.job4.PageRankCalculationReducer;
import PageRank.job5.PageRankOrderingMapper;
import PageRank.job5.PageRankOrderingReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
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
        master.BUCKET_NAME = args[0];
        master.RESULT_DIR = master.BUCKET_NAME + "/results";
        master.TMP_DIR = master.BUCKET_NAME + "/tmp";
//        master.buildInGraph("/data/enwiki-latest-pages-articles1.xml-p000000010p000010000", "/PageRank.inlink.out");
//        master.buildOutGraph("/PageRank.inlink.out", "/PageRank.outlink.out");
//        master.countPages("/PageRank.outlink.out", "/PageRank.n.out");
        master.getNFromFile("/PageRank.n.out");
        master.pageRankCalcInit("/PageRank.outlink.out", "/PageRank.iter0.out");

        for (int runs = 0; runs < 8; runs++) {
            master.calcuatePageRank(String.format("/PageRank.iter%d.out", runs), String.format("/PageRank.iter%d.out", runs + 1));
        }
        master.orderResultByPageRank("/PageRank.iter1.out", "/PageRank.iter1.out");
        master.orderResultByPageRank("/PageRank.iter8.out", "/PageRank.iter8.out");
    }


}

class PageRankMaster{
    public static String BUCKET_NAME = null;
    public static String RESULT_DIR = null;
    public static String TMP_DIR = null;
    public static long N = 0;

    public void buildInGraph(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path(BUCKET_NAME + inputPath);
        Path output = new Path(RESULT_DIR+outputPath);
        Path tmp = new Path(TMP_DIR+outputPath);

        Configuration conf = new Configuration();

        FileSystem inputFS = input.getFileSystem(conf);
        FileSystem outputFS = output.getFileSystem(conf);
        FileSystem tmpFS = tmp.getFileSystem(conf);

        // if input path does not exists, fail
        if (!inputFS.exists(input)) {
            System.out.println("Input file does not exist: " + input);
            System.exit(-1);
        }

        // if tmp path exists, delete recursively
        if (tmpFS.exists(tmp)) {
            tmpFS.delete(tmp, true);
        }

        // if output path exists, delete recursively
        if (outputFS.exists(output)) {
            outputFS.delete(output, true);
        }

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        // Create the actual job and run it.
        Job job = new Job(conf, "In-graph Generation");
        // finds the enclosing jar path
        job.setJarByClass(PageRankMaster.class);

        job.setInputFormatClass(XmlInputFormat.class);
        XmlInputFormat.setInputPaths(job, input);

        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setOutputPath(job, tmp);

        // our mapper class
        job.setMapperClass(AdjacencyGraphMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reducer class
        job.setReducerClass(AdjacencyGraphReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // run job and block until job is done, printing progress
        job.waitForCompletion(true);

        //merge file
        FileUtil.copyMerge(tmpFS, tmp, outputFS, output, true, conf, "");
    }

    public void buildOutGraph(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path(RESULT_DIR + inputPath);
        Path output = new Path(RESULT_DIR + outputPath);
        Path tmp = new Path(TMP_DIR + outputPath);

        Configuration conf = new Configuration();

        FileSystem inputFS = input.getFileSystem(conf);
        FileSystem outputFS = output.getFileSystem(conf);
        FileSystem tmpFS = tmp.getFileSystem(conf);

        // if input path does not exists, fail
        if (!inputFS.exists(input)) {
            System.out.println("Input file does not exist: " + input);
            System.exit(-1);
        }

        // if tmp path exists, delete recursively
        if (tmpFS.exists(tmp)) {
            tmpFS.delete(tmp, true);
        }

        // if output path exists, delete recursively
        if (outputFS.exists(output)) {
            outputFS.delete(output, true);
        }

        // Create the actual job and run it.
        Job job = new Job(conf, "Out-graph Generation");
        // finds the enclosing jar path
        job.setJarByClass(PageRankMaster.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        XmlInputFormat.setInputPaths(job, input);

        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setOutputPath(job, tmp);

        // our mapper class
        job.setMapperClass(AdjacencyOutGraphMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reducer class
        job.setReducerClass(AdjacencyOutGraphReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // run job and block until job is done, printing progress
        job.waitForCompletion(true);

        //merge file
        FileUtil.copyMerge(tmpFS, tmp, outputFS, output, true, conf, "");
    }

    public void countPages(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path(RESULT_DIR + inputPath);
        Path output = new Path(RESULT_DIR + outputPath);
        Path tmp = new Path(TMP_DIR + outputPath);

        Configuration conf = new Configuration();

        FileSystem inputFS = input.getFileSystem(conf);
        FileSystem outputFS = output.getFileSystem(conf);
        FileSystem tmpFS = tmp.getFileSystem(conf);

        // if input path does not exists, fail
        if (!inputFS.exists(input)) {
            System.out.println("Input file does not exist: " + input);
            System.exit(-1);
        }

        // if tmp path exists, delete recursively
        if (tmpFS.exists(tmp)) {
            tmpFS.delete(tmp, true);
        }

        // if output path exists, delete recursively
        if (outputFS.exists(output)) {
            outputFS.delete(output, true);
        }

        // Create the actual job and run it.
        Job job = new Job(conf, "Counter");
        // finds the enclosing jar path
        job.setJarByClass(PageRankMaster.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        XmlInputFormat.setInputPaths(job, input);

        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setOutputPath(job, tmp);

        // our mapper class
        job.setMapperClass(PageCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reducer class
        job.setReducerClass(PageCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // run job and block until job is done, printing progress
        job.waitForCompletion(true);

        //merge file
        FileUtil.copyMerge(tmpFS, tmp, outputFS, output, true, conf, "");
    }

    public void getNFromFile(String inputPath) throws IOException {
        Path path=new Path(RESULT_DIR + inputPath);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        N = Long.parseLong(line.trim().split("=")[1]);
    }

    public void pageRankCalcInit(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path(RESULT_DIR + inputPath);
        Path output = new Path(TMP_DIR + outputPath);
        Path tmp = new Path(TMP_DIR + "/tmp" + outputPath);

        Configuration conf = new Configuration();

        // Pass in our custom options.
        conf.set(Parameters.N_KEY, Long.toString(N));

        FileSystem inputFS = input.getFileSystem(conf);
        FileSystem outputFS = output.getFileSystem(conf);
        FileSystem tmpFS = tmp.getFileSystem(conf);

        // if input path does not exists, fail
        if (!inputFS.exists(input)) {
            System.out.println("Input file does not exist: " + input);
            System.exit(-1);
        }

        // if tmp path exists, delete recursively
        if (tmpFS.exists(tmp)) {
            tmpFS.delete(tmp, true);
        }

        // if output path exists, delete recursively
        if (outputFS.exists(output)) {
            outputFS.delete(output, true);
        }

        // Create the actual job and run it.
        Job job = new Job(conf, "Counter");
        // finds the enclosing jar path
        job.setJarByClass(PageRankMaster.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        XmlInputFormat.setInputPaths(job, input);

        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setOutputPath(job, tmp);

        // our mapper class
        job.setMapperClass(PageRankCalcInitMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);
        // run job and block until job is done, printing progress
        job.waitForCompletion(true);

        //merge file
        FileUtil.copyMerge(tmpFS, tmp, outputFS, output, true, conf, "");
    }

    public void calcuatePageRank(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path(TMP_DIR + inputPath);
        Path output = new Path(TMP_DIR + outputPath);
        Path tmp = new Path(TMP_DIR + "/tmp" + outputPath);

        Configuration conf = new Configuration();

        FileSystem inputFS = input.getFileSystem(conf);
        FileSystem outputFS = output.getFileSystem(conf);
        FileSystem tmpFS = tmp.getFileSystem(conf);

        // if input path does not exists, fail
        if (!inputFS.exists(input)) {
            System.out.println("Input file does not exist: " + input);
            System.exit(-1);
        }

        // if tmp path exists, delete recursively
        if (tmpFS.exists(tmp)) {
            tmpFS.delete(tmp, true);
        }

        // if output path exists, delete recursively
        if (outputFS.exists(output)) {
            outputFS.delete(output, true);
        }

        // Create the actual job and run it.
        Job job = new Job(conf, "PageRank.PageRank Calculation");
        // finds the enclosing jar path
        job.setJarByClass(PageRankMaster.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        XmlInputFormat.setInputPaths(job, input);

        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setOutputPath(job, tmp);

        // our mapper class
        job.setMapperClass(PageRankCalculationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reducer class
        job.setReducerClass(PageRankCalculationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // run job and block until job is done, printing progress
        job.waitForCompletion(true);

        //merge file
        FileUtil.copyMerge(tmpFS, tmp, outputFS, output, true, conf, "");
    }

    public void orderResultByPageRank(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path(TMP_DIR + inputPath);
        Path output = new Path(RESULT_DIR + outputPath);
        Path tmp = new Path(TMP_DIR + "/tmp2" + outputPath);

        // Create the job configuration
        Configuration conf = new Configuration();


        FileSystem inputFS = input.getFileSystem(conf);
        FileSystem outputFS = output.getFileSystem(conf);
        FileSystem tmpFS = tmp.getFileSystem(conf);

        // if input path does not exists, fail
        if (!inputFS.exists(input)) {
            System.out.println("Input file does not exist: " + input);
            System.exit(-1);
        }

        // if output path exists, delete recursively
        if (outputFS.exists(output)) {
            outputFS.delete(output, true);
        }

        // if tmp path exists, delete recursively
        if (tmpFS.exists(tmp)) {
            outputFS.delete(tmp, true);
        }

        // Create the actual job and run it.
        Job job = new Job(conf, "PageRank.PageRank ordering");
        // finds the enclosing jar path
        job.setJarByClass(PageRankMaster.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        org.apache.hadoop.mapreduce.lib.input.TextInputFormat.setInputPaths(job, input);

        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setOutputPath(job, tmp);

        // our mapper class
        job.setMapperClass(PageRankOrderingMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setSortComparatorClass(DescendingDoubleComparator.class);

        //reducer class
        job.setReducerClass(PageRankOrderingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // run job and block until job is done, printing progress
        job.waitForCompletion(true);

        //merge file
        FileUtil.copyMerge(tmpFS, tmp, outputFS, output, true, conf, "");
    }
}
