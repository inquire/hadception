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


public class NestingJoin extends Configured implements Tool{
       
public Path workingPath;
	
 public static class Map extends NestedMapper<LongWritable, Text, Text, IntWritable> {
    
	@Override
    protected void nestedMap (LongWritable key, Text value, JobTrigger condition) throws IOException, InterruptedException{

        writer.write(new Text(" "), value);
		condition.setCondition("sort");
	}
    
    
	@Override
	//@SuppressWarnings("unused")
	protected void setupNesting(Job job2, Configuration conf, JobTrigger condition) throws IOException{
    	job2.setJobName("IntermediateMaps");

    	job2.setNumReduceTasks(2);

    	job2.setOutputKeyClass(Integer.class);
    	job2.setOutputValueClass(Text.class);

    	job2.setMapperClass(GetKeyMap.class);

    	job2.setInputFormatClass(TextInputFormat.class);
    	job2.setOutputFormatClass(SequenceFileOutputFormat.class);

    	job2.setJarByClass(NestingOrderSum.class);
    	
    	if (condition.getCondition().equals("sort")){
    		job2.setJobName("NOM-Sorting-Layer");
    	}
	}
 }   

 public static class GetKeyMap extends Mapper<LongWritable, Text, Text, Text> {
	    
	   protected void nestedMap (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		   String line = value.toString();
	       StringTokenizer tokenizer = new StringTokenizer(line);
	       String record = "";
	       String keyRecord = "";
	       
	       for (int i = 0; i<14; i++){
	    	   if(i == 13){
	       		keyRecord = tokenizer.nextToken(",");
	       	}else{
	       		record = record + tokenizer.nextToken(",") + " ";
	       	}
	       }
	       	context.write(new Text(keyRecord), new Text(record));
		}
	}

 
 	public int run(String[] args) throws Exception {

 		Job job = new Job();
 		
 		job.setJobName("NestingJoin");

 		job.setOutputKeyClass(Text.class);
 		job.setOutputValueClass(Text.class);

 		job.setMapperClass(Map.class);

 		job.setInputFormatClass(TextInputFormat.class);
 		job.setOutputFormatClass(TextOutputFormat.class);
 		
 		FileInputFormat.setInputPaths(job, new Path(args[0]));
 		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 		
		job.setJarByClass(NestingJoin.class);
	
		job.submit();
		return 0;
	}

 	public static void main(String[] args) throws Exception {
 		Configuration conf = new Configuration();
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		ToolRunner.run(new NestingJoin(), otherArgs);
 	}
}