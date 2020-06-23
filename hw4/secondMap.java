package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class secondMap extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split(";");
        String[] words1;
        if(words.length<4){
            words1=new String[4];
            if(words.length==2){
               words1[2]=words[1];
               if(words[1].charAt(words[1].length()-2)=='1'){
                   words1[3]=words[1];
                           words1[3].replace(words[1].charAt(words[1].length()-2),'0');
               }
               words1[3]=words[1];
            }
            else if(words.length==3){
                words1[2]=words[1];
                if(words[1].charAt(words[1].length()-2)=='1'){
                    words1[3]=words[2];
                    words1[3].replace(words[2].charAt(words[2].length()-2),'0');
                }
            }
        }
        else {
            words1 = new String[words.length];
        }

            for (int i = 1; i < words.length-2; i++) {
                String word=words[i].replaceAll("[^a-zA-Z0-9%*2c]","");
                String word1=words[i+1].replaceAll("[^a-zA-Z0-9%*2c]","");
                String word2=words[i+2].replaceAll("[^a-zA-Z0-9%*2c]","");

                if(word.charAt(word.length()-1)=='0'){
                    word=word.substring(0,word.length()-1);
                }
                if(word1.charAt(word1.length()-1)=='0'){
                    word1=word1.substring(0,word1.length()-1);
                }
                words1[i] = word+"," + word1;
                if(word2.charAt(word2.length()-1)=='1') {
                    word2=word2.substring(0,word2.length()-1);
                    context.write(new Text(words1[i]), new Text(word2));
                }
            }

    }
}