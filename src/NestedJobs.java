

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


@Deprecated
public class NestedJobs {
	
	private String nestedName = "Nested Job"; // needs detailed path & implementation
	
	Configuration conf = new Configuration();
	Job nestedJob = new Job(conf, nestedName);
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public NestedJobs(Class<?> Maps, Path input, Path output)
			throws IOException, InterruptedException{
		
		nestedJob.setInputFormatClass(SequenceFileInputFormat.class);
		nestedJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		nestedJob.setMapperClass((Class<? extends Mapper>) Maps);
		
		nestedJob.setOutputKeyClass(Text.class);
 		nestedJob.setOutputValueClass(IntWritable.class);
		
		SequenceFileInputFormat.addInputPath(nestedJob,input);
		SequenceFileOutputFormat.setOutputPath(nestedJob, output);
		
		//nestedJob.setJarByClass(MRMain.class);
	}


	public void run() throws IOException, InterruptedException{
		try{
			nestedJob.waitForCompletion(true);
		}catch(ClassNotFoundException ex){
			System.err.print(ex.getStackTrace());
		}
	}
	
	

}
