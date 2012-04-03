import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Mapper.Context;
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


public class NewNestSort extends Configured implements Tool{
       
public Path workingPath;
	
 public static class Map extends NestedMapper<LongWritable, Text, IntWritable, Text> {
    
	@Override
    protected void nestedMap (LongWritable key, Text value, JobTrigger condition) throws IOException, InterruptedException{
			condition.setCondition("sort");
			writer.write(new Text(""), value);
	}
    
    
	@Override
	//@SuppressWarnings("unused")
	protected void setupNesting(Job job2, Configuration conf, JobTrigger condition) throws IOException{
    	job2.setJobName("IntermediateMaps");

    	job2.setNumReduceTasks(1);

    	job2.setOutputKeyClass(IntWritable.class);
    	job2.setOutputValueClass(Text.class);

    	job2.setMapperClass(GetKeyMap.class);

    	job2.setInputFormatClass(SequenceFileInputFormat.class);
    	job2.setOutputFormatClass(SequenceFileOutputFormat.class);

    	job2.setJarByClass(NewNestSort.class);
    	
    	if (condition.getCondition().equals("sort")){
    		job2.setJobName("NOM-Sorting-Layer");
    	}
	}
 }   
 
 public static class GetKeyMap extends Mapper<Text, Text, IntWritable, Text> {
	public void map (Text key, Text value, Context context) throws IOException, InterruptedException{
	    String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      String record = " ";
      int sum = 0;

      for (int i = 0; i<tokenizer.countTokens(); i++){
      	if((i >= 2) && (i <= 9)){
      		sum = sum + Integer.valueOf(tokenizer.nextToken(", "));
      	}
			record = record + tokenizer.nextToken(",") + " ";
      }
      context.write(new IntWritable(sum), new Text(record));
								
	}
}
 
 	public int run(String[] args) throws Exception {

 		Job job = new Job();
 		
 		job.setJobName("NestingOrderSum-blockx20");

 		job.setOutputKeyClass(IntWritable.class);
 		job.setOutputValueClass(Text.class);

 		job.setMapperClass(Map.class);
		job.setNumReduceTasks(15);
		
 		job.setInputFormatClass(TextInputFormat.class);
 		job.setOutputFormatClass(TextOutputFormat.class);
 		
 		FileInputFormat.addInputPath(job, new Path(args[0]));
 		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 		
		job.setJarByClass(NewNestSort.class);
	
		job.submit();
		return 0;
	}

 	public static void main(String[] args) throws Exception {
 		Configuration conf = new Configuration();
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		ToolRunner.run(new NewNestSort(), otherArgs);
 	}
}