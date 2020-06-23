package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class thirdReduce extends Reducer<IntWritable, Text, Text, IntWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            int key1=key.get();
            key1=key1*(-1);
            key.set(key1);
            context.write(new Text(value), key);
        }
    }
}