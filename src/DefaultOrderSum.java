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

import uk.ac.ed.inf.nmr.mapreduce.JobTrigger;
import uk.ac.ed.inf.nmr.mapreduce.NestedMapper;

public class DefaultOrderSum extends Configured implements Tool{
       
public Path workingPath;
	
public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
    
   public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
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

 		job.setJobName("DefaultOrderSum-blockx20");
 		
 		job.setOutputKeyClass(IntWritable.class);
 		job.setOutputValueClass(Text.class);

 		job.setMapperClass(Map.class);
		job.setNumReduceTasks(15);


 		job.setInputFormatClass(TextInputFormat.class);
 		job.setOutputFormatClass(TextOutputFormat.class);
 		
 		FileInputFormat.addInputPath(job, new Path(args[0]));
 		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 		
		job.setJarByClass(DefaultOrderSum.class);
	
		job.submit();
		return 0;
	}

 	public static void main(String[] args) throws Exception {
 		Configuration conf = new Configuration();
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		ToolRunner.run(new DefaultOrderSum(), otherArgs);
 	}
 }



