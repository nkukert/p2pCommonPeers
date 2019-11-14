// import java.io.IOException;
// import java.util.StringTokenizer;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;

public class peerCount {
    public static class TokenizerMapper
        extends Mapper<LongWritable, Text, Text, Text> {
            //creates a mapper that takes a KV pair of Object, Text, and turns it into 
            //a pair of text, integer (in other words, takes the string and text and makes it
            //text)

            //private final static IntWritable one = new IntWritable(1);
            private Text pair = new Text(); //output key
            private Text list = new Text();
            //private Text word = new Text();

            public void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException{
            
                    // StringTokenizer itr = new StringTokenizer(value.toString());
                    // while (itr.hasMoreTokens()) {
                    //     //make each string a token to facilitate stripping away of first instance of peer
                    //     word.set(itr.nextToken("\\r?\\n"));
                    //     context.write(word, one);
                    // }

                    String[] line = value.toString().split("\\r");
                    String user = line[0];
                    if (line.length > 1) {
                        ArrayList<String> neighborList = new ArrayList<String>(Arrays.asList(line[1].split("\\")));
                        for(String neighbor:neighborList){
                            String neighborPair = (Integer.parseInt(user) < Integer.parseInt(neighbor))?user+" "+neighbor:neighbor+" "+user;
                            ArrayList<String> temp = new ArrayList<String>(neighborList);
                            temp.remove(neighbor);
                            String listString = String.join(",", temp);
                            pair.set(neighborPair);
                            list.set(listString);
                            context.write(pair, list);
                        }
                    }
                }
    }



    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public String Mutual(String s1, String s2, int i){
            HashSet<String> map = new HashSet<String>();
            String[] s1_split = s1.split("\\,");
            String[] s2_split = s2.split("\\,");
            String result = "";
            for(String s:s1_split){
                map.add(s);
            }
            for(String s:s2_split){
                if(map.contains(s)){
                    result +=s+",";
                }
            }
            return result;
        }
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] neighbor_key_values = new String[2];
            int i = 0;
            for(Text val : values) {
                neighbor_key_values[i++] = val.toString();
            }

            result.set(Mutual(neighbor_key_values[0], neighbor_key_values[1],i));
            i++;
            context.write(key, result); //create a pair of <keyword, number of occurrences>
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(peerCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}