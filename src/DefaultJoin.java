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

public class DefaultJoin extends Configured implements Tool{
       
public Path workingPath;
	
public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    
   public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	   	 String line = value.toString();
       StringTokenizer tokenizer = new StringTokenizer(line);
       String record = "";
       String keyRecord = "";
       
       for (int i = 0; i<14; i++){
    	   if(i == 13){
       		keyRecord = tokenizer.nextToken(" ");
       	}else{
       		record = record + tokenizer.nextToken(" ") + " ";
       	}
       }
       	context.write(new Text(keyRecord), new Text(record));
	}
}

 	public int run(String[] args) throws Exception {

 		Job job = new Job();

 		job.setJobName("DefaultJoin-block");
 		
 		job.setOutputKeyClass(Text.class);
 		job.setOutputValueClass(Text.class);

 		job.setMapperClass(Map.class);

 		job.setInputFormatClass(TextInputFormat.class);
 		job.setOutputFormatClass(TextOutputFormat.class);
 		
 		FileInputFormat.addInputPath(job, new Path(args[0]));
 		//FileInputFormat.addInputPath(job, new Path("/user/s0838600/experiments/input/block"));
		FileInputFormat.addInputPath(job, new Path("/user/s0838600/experiments/input/hdfs-blockx10"));
 		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 		
		job.setJarByClass(DefaultTest.class);
	
		job.setNumReduceTasks(11);
	
		job.submit();
		return 0;
	}

 	public static void main(String[] args) throws Exception {
 		Configuration conf = new Configuration();
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		ToolRunner.run(new DefaultJoin(), otherArgs);
 	}
 }


