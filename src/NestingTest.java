import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

import uk.ac.ed.inf.nmr.mapreduce.JobTrigger;
import uk.ac.ed.inf.nmr.mapreduce.NestedMapper;
import uk.ac.ed.inf.nmr.mapreduce.NestedReducer;


public class NestingTest extends Configured implements Tool{
       
public Path workingPath;
	
 public static class Map extends NestedMapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
	@Override
	//@SuppressWarnings("unused")
	protected void setupNesting(Job job2, Configuration conf, JobTrigger condition) throws IOException{
    	job2.setJobName("IntermediateMaps");
	}

    
    @Override
    public void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
    	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken().toUpperCase());
            context.write(word, one);
        }
    }
 }        
   
 public static class Reduce extends NestedReducer<Text, IntWritable, Text, IntWritable> {

	 @Override
	 protected void nestedReducer(Text key, Iterable<IntWritable> values, JobTrigger condition)
	 	throws IOException, InterruptedException{
		 
		 int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }

	       writer.write(key, new IntWritable(sum));
	 }
	 
	 @Override
	 protected void setupNesting(Job job2, Configuration conf, JobTrigger condition) throws IOException{

		 job2.setJobName("IntermediateReducers");

	 } 
 }
 
 	public int run(String[] args) throws Exception {

 		Job job = new Job();
 		
 		job.setJobName("NestingTest-blockx15");

 		job.setOutputKeyClass(Text.class);
 		job.setOutputValueClass(IntWritable.class);

 		job.setMapperClass(Map.class);
 		job.setReducerClass(Reduce.class);

 		job.setInputFormatClass(TextInputFormat.class);
 		job.setOutputFormatClass(TextOutputFormat.class);
 		
 		job.setNumReduceTasks(15);
 		
 		FileInputFormat.setInputPaths(job, new Path(args[0]));
 		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 		
		job.setJarByClass(MRMain.class);
	
		job.submit();
		return 0;
	}

 	public static void main(String[] args) throws Exception {
 		Configuration conf = new Configuration();
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		ToolRunner.run(new NestingTest(), otherArgs);
 	}
}
