package com.mycompany1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;


@SuppressWarnings("deprecation")
public class Test_BD {
    @SuppressWarnings("deprecation")
	public static class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, DBOutput, IntWritable> {
        private static IntWritable one = new IntWritable(1);
        private static DBOutput text = new DBOutput();
        
        public void map(LongWritable key, Text value,
                OutputCollector<DBOutput, IntWritable> collect, Reporter arg3)
                throws IOException {
            StringTokenizer token = new StringTokenizer(value.toString());
            while(token.hasMoreTokens()) {
                text.setText(token.nextToken());
                collect.collect(text, one);
            }            
                        
        }
        
    }
    
    @SuppressWarnings("deprecation")
	public static class WordCountReducer extends MapReduceBase implements Reducer<DBOutput, IntWritable, DBOutput, IntWritable> {

        


        public void reduce(DBOutput key, Iterator<IntWritable> values,
                OutputCollector<DBOutput, IntWritable> collect, Reporter arg3)
                throws IOException {
            int sum = 0;
            IntWritable no = null;
            DBOutput dbKey = new DBOutput();
            
            while(values.hasNext()) {
                no = values.next();
                sum += no.get();
            }
            dbKey.setText(key.getText());
            dbKey.setNo(sum);
            collect.collect(dbKey, new IntWritable(sum));
            
        }
        
    }
    
    @SuppressWarnings("deprecation")
	public void run(String inputPath, String outputPath) throws Exception {
        JobConf conf = new JobConf(Test_BD.class);
        conf.setJobName("wordcount");
        DistributedCache.addFileToClassPath(new Path("ojdbc/ojdbc6.jar"), conf);

        // the keys are DBOutput
        conf.setOutputKeyClass(DBOutput.class);
        // the values are counts (ints)
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordCountMapper.class);
        conf.setReducerClass(WordCountReducer.class);        
        
        conf.setOutputFormat(DBOutputFormat.class);

        FileInputFormat.addInputPath(conf, new Path(inputPath));
        DBOutputFormat.setOutput(conf, "word_count", "word", "count");
        
        DBConfiguration.configureDB(conf, "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@192.168.72.129:1521/remot", "mohamed", "mohamed");
        
        //FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        JobClient.runJob(conf);
      }
    
    public static void main(String[] args) throws Exception {
    	Test_BD wordCount = new Test_BD();
        wordCount.run(args[0], args[1]);
    }
    
    private static class DBOutput implements DBWritable, WritableComparable<DBOutput> {
        
        private String text;
        
        private int no;

        
        public void readFields(ResultSet rs) throws SQLException {
            text = rs.getString("word");
            no = rs.getInt("count");
        }

        public void write(PreparedStatement ps) throws SQLException {
            ps.setString(1, text);
            ps.setInt(2, no);
        }
        
        public void setText(String text) {
            this.text = text;
        }
        
        public String getText() {
            return text;
        }
        
        public void setNo(int no) {
            this.no = no;
        }
        
        @SuppressWarnings("unused")
		public int getNo() {
            return no;
        }

        public void readFields(DataInput input) throws IOException {
            text = input.readUTF();    
            no = input.readInt();
        }

        public void write(DataOutput output) throws IOException {
            output.writeUTF(text);
            output.writeInt(no);
        }

        public int compareTo(DBOutput o) {
            return text.compareTo(o.getText());
        }
        
    }
}