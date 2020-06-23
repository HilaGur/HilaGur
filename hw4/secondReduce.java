package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.StringWriter;

public class  secondReduce extends Reducer<Text, Text, Text , LongWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long count = 0;
        for (Text value : values) {
                count++;

        }
        if(count>0)
            context.write(key , new LongWritable(count));

    }
}
