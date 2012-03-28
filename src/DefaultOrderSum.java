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
    
   protected void nestedMap (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	   String line = value.toString();
       StringTokenizer tokenizer = new StringTokenizer(line);
       String record = null;
       int sum = 0;
       
       for (int i = 0; i<tokenizer.countTokens(); i++){
       	if((i >= 2) && (i <= 9)){
       		sum = sum + Integer.valueOf(tokenizer.nextToken(","));
       	}else{
       		record = record + tokenizer.nextToken(",");
       	}
       }
       	context.write(new IntWritable(sum), new Text(record));
	}
}

 	public int run(String[] args) throws Exception {

 		Job job = new Job();

 		job.setJobName("DefaultTest");
 		
 		job.setOutputKeyClass(IntWritable.class);
 		job.setOutputValueClass(Text.class);

 		job.setMapperClass(Map.class);

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
 		ToolRunner.run(new DefaultOrderSum(), otherArgs);
 	}
 }



