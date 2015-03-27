package com.mycompany.Test_hadoop_oracle;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class App {
    /**
     * The map class of WordCount.
     */
    public static class TokenCounterMapper
        extends Mapper<Object, Text, Text, IntWritable> {
 
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
 
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString()); //to split a string by â€œspaceâ€? and â€œcommaâ€? delimiter, 
                                                                           //and iterate the StringTokenizer elements and print it out one by one.
           while (itr.hasMoreTokens()) {
        	   //Determines whether there are more tokens in the string to be parsed.
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    /**
     * The reducer class of WordCount
     */
    public static class TokenCounterReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    /**
     * The main entry point.
     */
    public static void main(String[] args) throws Exception {
        String[] argsTemp = {"input_data","output_data"};
 
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, argsTemp).getRemainingArgs();
        Job job = new Job(conf, "Example Hadoop 0.20.0 WordCount");
        job.setJarByClass(App.class);
        //sets the jar file in which each node will look for the Mapper and Reducer classes.
        job.setMapperClass(TokenCounterMapper.class);
        job.setReducerClass(TokenCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
     // Execute the job and wait for it to complete
    }
}
