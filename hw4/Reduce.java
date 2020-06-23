package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.StringWriter;

public class  Reduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            String name = "";
            for (Text t : values)
            {
//                    String parts[] = t.toString().split(",");
                    name = name+";"+t.toString();
            }

            context.write(key, new Text(name));

    }
}
