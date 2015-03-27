//package com.hadoop;
//
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
//import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
//import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
//
//public class Main
//{
//   public static void main(String[] args) throws Exception
//   {
//     Configuration conf = new Configuration();
//     DBConfiguration.configureDB(conf,
//     "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@192.168.72.129:1521/remot", "mohamed", "mohamed");
//
//     Job job = new Job(conf);
//     job.setJarByClass(Main.class);
//     job.setMapperClass(Map.class);
//     job.setReducerClass(Reduce.class);
//     job.setMapOutputKeyClass(Text.class);
//     job.setMapOutputValueClass(IntWritable.class);
//     job.setOutputKeyClass(DBOutputWritable.class);
//     job.setOutputValueClass(NullWritable.class);
//     job.setInputFormatClass(DBInputFormat.class);
//     job.setOutputFormatClass(DBOutputFormat.class);
//
//     DBInputFormat.setInput(
//     job,
//     DBInputWritable.class,
//     "studentinfo",   //input table name
//     null,
//     null,
//     new String[] { "id", "name" }  // table columns
//     );
//
//     DBOutputFormat.setOutput(
//     job,
//     "output",    // output table name
//     new String[] { "name", "count" }   //table columns
//     );
//
//     System.exit(job.waitForCompletion(true) ? 0 : 1);
//   }
//}