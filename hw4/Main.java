package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Random;

public class Main extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Path tempDir1 = new Path("data/temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Path tempDir2 = new Path("data/temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Configuration conf = getConf();
        FileSystem.get(conf).delete(new Path("data/output"), true);

        try {
// ----------- FIRST JOB -----------
            System.out.println("-------FIRST JOB-------");
            Job wordCountJob = Job.getInstance(conf);
            wordCountJob.setJobName("wordcount");
            wordCountJob.setJarByClass(Main.class);

            wordCountJob.setMapOutputValueClass(Text.class);
            wordCountJob.setOutputKeyClass(Text.class);
            wordCountJob.setOutputValueClass(Text.class);

            wordCountJob.setMapperClass(Map.class);
            wordCountJob.setReducerClass(Reduce.class);

            Path inputFilePath = new Path("data/input/hw/");
            Path outputFilePath = tempDir1;

            FileInputFormat.addInputPath(wordCountJob, inputFilePath);
            FileOutputFormat.setOutputPath(wordCountJob, outputFilePath);

            if (!wordCountJob.waitForCompletion(true)) {
                return 1;
            }

// ----------- SECOND JOB -----------
            System.out.println("-------SECOND JOB-------");
            conf = new Configuration();
            Job filterJob = Job.getInstance(conf);
            filterJob.setJobName("Filter");

            filterJob.setJarByClass(Main.class);
            FileInputFormat.setInputPaths(filterJob, tempDir1);
            outputFilePath = tempDir2;
            FileOutputFormat.setOutputPath(filterJob,tempDir2);

            filterJob.setMapOutputValueClass(Text.class);
            filterJob.setOutputKeyClass(Text.class);
            filterJob.setOutputValueClass(LongWritable.class);

            filterJob.setMapperClass(secondMap.class);
            filterJob.setReducerClass(secondReduce.class);


            if (!filterJob.waitForCompletion(true)) {
                return 1;
            }
            System.out.println("-------THIRD JOB-------");
            Job thirdJob = Job.getInstance(conf);
            thirdJob.setJobName("Third");

            FileInputFormat.setInputPaths(thirdJob, tempDir2);
            FileOutputFormat.setOutputPath(thirdJob, new Path("data/output"));

            thirdJob.setMapOutputValueClass(Text.class);
            thirdJob.setOutputKeyClass(IntWritable.class);
            thirdJob.setOutputValueClass(Text.class);

            thirdJob.setMapperClass(thirdMap.class);
            thirdJob.setReducerClass(thirdReduce.class);

            return thirdJob.waitForCompletion(false) ? 0 : 1;

        }
        finally {
            FileSystem.get(conf).delete(tempDir1, true);
            FileSystem.get(conf).delete(tempDir2, true);
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }
}

