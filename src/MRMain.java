
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


 

public class MRMain extends Configured implements Tool{
       
public Path workingPath;
	
 public static class Map extends NestedMapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken().toUpperCase());
            context.write(word, one);
        }
    }
    
    
    @Override
    //@SuppressWarnings("unused")
	protected void setupNesting(Job job2, Configuration conf) throws IOException{
    	//job2 = new Job(conf, "Layer2");
    	
        job2.setOutputKeyClass(LongWritable.class); // modified here
        job2.setOutputValueClass(Text.class);		// modified here
        job2.setMapperClass(FinalMap.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
  
    	
    }
    
    @Override
    protected void nestedMap (LongWritable key, Text value) throws IOException, InterruptedException{
		System.out.println(key + " / " + value);
		  writer.write(key, value);
	  }
 }    
    
 
 public static class FinalMap extends Mapper<LongWritable, Text, LongWritable, Text>{
	 
	 public void map (LongWritable key, Text value, Context context)
			 throws IOException, InterruptedException{
		 context.write(key, value);
	 }
	 
 }
    
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
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

 		job.setInputFormatClass(TextInputFormat.class);
 		job.setOutputFormatClass(TextOutputFormat.class);
 		
 		FileInputFormat.setInputPaths(job, new Path(args[0]));
 		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setJarByClass(MRMain.class);
	
		job.submit();
		return 0;
	}

 	public static void main(String[] args) throws Exception {
 		Configuration conf = new Configuration();
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		ToolRunner.run(new MRMain(), otherArgs);
 	}
}
