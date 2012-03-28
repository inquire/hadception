import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

public class DefaultTest extends Configured implements Tool{
       
public Path workingPath;
	
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

   
	protected Map (Text key, IntWritable value, Context context) throws IOException, InterruptedException{
    	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken().toUpperCase());
        	System.out.println("Here we go with weird stuff: " + word +" / "+ one);

            context.write(word, one);
	  }
	}
 }    
    
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	 
	 @Override
	 protected void reduce(Text key, Iterable<IntWritable> values, Context context)
	 	throws IOException, InterruptedException{
		 
		 int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	       context.write(key, new IntWritable(sum));
	 } 
 }
    

 	public int run(String[] args) throws Exception {

 		Job job = new Job();

 		job.setOutputKeyClass(Text.class);
 		job.setOutputValueClass(IntWritable.class);

 		job.setMapperClass(Map.class);
 		job.setReducerClass(Reduce.class);

		job.setNumReduceTasks(1);
		
 		job.setInputFormatClass(TextInputFormat.class);
 		job.setOutputFormatClass(TextOutputFormat.class);
 		
 		FileInputFormat.setInputPaths(job, new Path(args[0]));
 		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 		
		job.setJarByClass(DefaultTest.class);
	
		job.submit();
		return 0;
	}

 	public static void main(String[] args) throws Exception {
 		Configuration conf = new Configuration();
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		ToolRunner.run(new DefaultTest(), otherArgs);
 	}
 }



