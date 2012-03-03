
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
	
 public static class Map extends BetaMapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

   
		@Override
    protected void nestedMap (LongWritable key, Text value, String condition) throws IOException, InterruptedException{
		System.out.println(key + " / " + value);
		  writer.write(value, " ");
		  condition = "something";
	  }
    
		@Override
	    //@SuppressWarnings("unused")
		protected void setupNesting(Job job2, Configuration conf, String condition) throws IOException{
	    //job2 = new Job(conf, "Layer2");

    	job2.setJobName("Layer-2-Mapper-No");

    	job2.setNumReduceTasks(1);

      job2.setOutputKeyClass(LongWritable.class); // modified here
      job2.setOutputValueClass(Text.class);		// modified here

      job2.setMapperClass(FinalMapM.class);

      job2.setInputFormatClass(TextInputFormat.class);
      job2.setOutputFormatClass(SequenceFileOutputFormat.class);

      if (condition == "something"){
      	FileInputFormat.addInputPath(job2, new Path("/tmp/nesten"));
          job2.setJobName("Layer-2-Mapper-Yes");
      }
		}

    
    @Override
    public void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
        
    	System.out.println(key +" / "+ value);
    	
    	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken().toUpperCase());
            context.write(word, one);
        }
    }
 }    
    
 
 public static class FinalMapM extends Mapper<LongWritable, Text, LongWritable, Text>{
	 //  private final static IntWritable one = new IntWritable(1);
	 //  private Text word = new Text();
	 
	 /*
	   @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        
	    	System.out.println(key +" / "+ value);
	    	
	    	String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        
	        while (tokenizer.hasMoreTokens()) {
	            word.set(tokenizer.nextToken().toUpperCase());
	            context.write(word, one);
	        }
	    }
	 */
	 public void map (LongWritable key, Text value, Context context)
			 throws IOException, InterruptedException{
		 
		 System.out.println("Stuff is going on here: " + key.toString() + " / " + value.toString());
		 context.write(key, value);
	 }
	 
 }
    
 public static class Reduce extends BetaReducer<Text, IntWritable, Text, IntWritable> {

	 @Override
	 protected void nestedReducer(Text key, Iterable<IntWritable> values, String condition)
	 	throws IOException, InterruptedException{
		 
		 int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	       writer.write(key, new IntWritable(sum));
		 
	 }
	 
	 
	 @Override
	 //@SuppressWarnings("unused")
	 protected void setupNesting(Job job2, Configuration conf, String condition) throws IOException{
		 //job2 = new Job(conf, "Layer2");

		 job2.setJobName("Layer-2-Reducer-No");

		 job2.setOutputKeyClass(Text.class); // modified here
		 job2.setOutputValueClass(IntWritable.class);		// modified here

		 job2.setMapperClass(FinalMapR.class);

		 job2.setInputFormatClass(SequenceFileInputFormat.class);
		 job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		 if (condition != "somethin]g"){
			 //	FileInputFormat.addInputPath(job2, new Path("/tmp/nesten"));
			 job2.setJobName("Layer-2-Reducer-Yes");
		 }

	 } 
	 
	/** 
	@Override 
    protected void reduce(Writable key, Writable value, Context context) 
      throws IOException, InterruptedException {
      
       context.write((KEYOUT)key, (VALUEOUT)value);
    }
  	*/
 }
 
 public static class FinalMapR extends Mapper<Text, IntWritable, Text, IntWritable>{
	 
	 public void map (Text key, IntWritable value, Context context)
			 throws IOException, InterruptedException{
		 
		 System.out.println("Stuff is going on here: " + key.toString() + " / " + value.toString());
		 context.write(key, value);
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
