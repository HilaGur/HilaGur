package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split(",");
        String[] words1 = {words[1],"("+ words[2] + "," + words[3]+")"};
        //for (String word : words1) {
            context.write(new Text(words1[0]), new Text(words1[1]));

    }
}
