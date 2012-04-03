import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;


import uk.ac.ed.inf.nmr.mapreduce.JobTrigger;
import uk.ac.ed.inf.nmr.mapreduce.NestedMapper;
import uk.ac.ed.inf.nmr.mapreduce.NestedReducer;


public class NestedCounter extends Configured implements Tool{
       
public Path workingPath;
	
 public static class Map extends NestedMapper<LongWritable, Text, Text, IntWritable> {
    
	 @Override
	 protected void nestedMap (LongWritable key, Text value, JobTrigger condition) throws IOException, InterruptedException{
		 writer.write(new Text(" "), new Text (value));
		condition.setCondition("sort");
	 }
		
	@Override
	//@SuppressWarnings("unused")
	protected void setupNesting(Job job2, Configuration conf, JobTrigger condition) throws IOException{
    	job2.setJobName("NMR-Counter-blockx10");

    	job2.setNumReduceTasks(11);

    	job2.setOutputKeyClass(Text.class);
    	job2.setOutputValueClass(IntWritable.class);

    	job2.setMapperClass(NMRMap.class);
    	job2.setReducerClass(NMRReduce.class);

    	job2.setInputFormatClass(TextInputFormat.class);
    	job2.setOutputFormatClass(SequenceFileOutputFormat.class);



    	job2.setJarByClass(NestedCounter.class);
    	
    	if (condition.getCondition().equals("sort")){
    		job2.setJobName("NOM-Sorting-Layer");
				FileInputFormat.setInputPaths(job2, new Path("/user/s0838600/experiments/input/hdfs-blockx10"));
    		condition.setCondition("something else");
    	}
	}
 }        
 
 public static class NMRMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	   private final static IntWritable one = new IntWritable(1);
	   private Text word = new Text();
	       
	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	       String line = value.toString();
	       StringTokenizer tokenizer = new StringTokenizer(line);
	       //while (tokenizer.hasMoreTokens()) {
       	for (int i = 0; i < 3; i++){
	    			 word.set(tokenizer.nextToken());
	           context.write(word, one);
	       }
	   }
	} 
	       
	public static class NMRReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	   public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	     throws IOException, InterruptedException {
	       int sum = 0;
				 if (key == new Text("6, ")){
	       for (IntWritable val : values) {
	           sum += val.get();
	       }
					context.write(key, new IntWritable(sum));
				}
	   }
	}
   
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	 @Override
	 public void reduce(Text key, Iterable<IntWritable> values, Context context)
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
 		
 		job.setJobName("NestingCounter-block");

 		job.setOutputKeyClass(Text.class);
 		job.setOutputValueClass(IntWritable.class);

 		job.setMapperClass(Map.class);
 		job.setReducerClass(Reduce.class);

 		job.setInputFormatClass(TextInputFormat.class);
 		job.setOutputFormatClass(TextOutputFormat.class);
 		
 		job.setNumReduceTasks(1);
 		
 		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path("/user/s0838600/experiments/input/hdfs-block"));
 		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 		
		job.setJarByClass(NestedCounter.class);
	
		job.submit();
		return 0;
	}

 	public static void main(String[] args) throws Exception {
 		Configuration conf = new Configuration();
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		ToolRunner.run(new NestedCounter(), otherArgs);
 	}
}
