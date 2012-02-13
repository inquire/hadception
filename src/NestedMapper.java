import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.mapred.join.InnerJoinRecordReader;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.RawComparator;
//import org.apache.hadoop.io.compress.CompressionCodec;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;





public class NestedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	Class<?> myClass;
	
	  /**
	   * Called once at the beginning of the task. 
	   * It contains the: conf, taskid, reader, writer, commited, reporter, split.
	   */
	
	  protected void setup(Context context
              ) throws IOException, InterruptedException {
		  // NOTHING
	  }
	  
	  /** 
	   * Called once for each key/value pair in the input split. Most applications
	   * should override this, but the default is the identity function.
	   */
	  
	  @SuppressWarnings("unchecked")
	  protected void map(KEYIN key, VALUEIN value, 
	                     Context context) throws IOException, InterruptedException {  
	    context.write((KEYOUT)key, (VALUEOUT)value);
	  }
	  
	  /**
	   * Called once at the end of the task.
	   * Performs maintenance of the task, removing all redundant structures. 
	   */
	  protected void cleanup(Context context
	                         ) throws IOException, InterruptedException {
	    // NOTHING
	  }
	  
	  /**
	   * Core of the mapper, section responsible for setting up the context
	   * and running the user supplied functions. 
	 * @throws InterruptedException 
	 * @throws IOException 
	   */
	  
	  // Hacked Implementation 


	NestedWriterSF<KEYIN, VALUEIN> writer;
	protected void setupNestedMap(Context context) throws IOException, InterruptedException{
	
		try {
			writer =  new NestedWriterSF<KEYIN, VALUEIN>(context);
		} catch (Exception e) {
			e.printStackTrace();
		}
	  
		  
		  while(context.nextKeyValue()){
			  nestedMap(context.getCurrentKey(), context.getCurrentValue());
		  }
		  
		  writer.close();
		  while(runNestedJob() != true){
			  // we wait;
		  }
		  
	  }
	// HACKED IMPLEMENTATION
	/* 
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected boolean nextMapSpecs() throws IOException, InterruptedException{
    	
    	Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "Layer2");
    	
        job2.setOutputKeyClass(LongWritable.class); // modify stuff
        job2.setOutputValueClass(Text.class); // modify stuff
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        
        FileInputFormat.addInputPath(job2, writer.getPath());
        FileOutputFormat.setOutputPath(job2, new Path("/tmp/outputs/2"));

        job2.setJarByClass(MRMain.class);
        try{
			System.out.println("before");
        	job2.waitForCompletion(true);
        }catch(ClassNotFoundException ex){
        	System.out.println(ex);
        }    	
        
        return true;
    }
	*/
	
	protected boolean runNestedJob() throws IOException, InterruptedException{
		// Custom user implementation
		return true;
	}
	
	
	protected void nestedMap (KEYIN key, VALUEIN value) throws IOException, InterruptedException{
		System.out.println(key + " / " + value);
		  writer.write((KEYIN) key, (VALUEIN) value);
	}
	

	  
	  NestedReaderSF reader;
	  @SuppressWarnings("unchecked")
	protected void setupNormalMap(Context context) throws IOException, InterruptedException{
		  reader = new NestedReaderSF(context);
		  
		  while (reader.next()){
			  map((KEYIN) reader.getKey(), (VALUEIN) reader.getValue(), context);
		  }
		  
	  }
	  
	  
	protected void runJob(Class<?> map) throws IOException, InterruptedException{
		NestedJobs newJob = new NestedJobs(map, writer.getPath(), new Path("/tmp/outputs/2/"));
		newJob.run();
	}
	  
	  public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    
		    	// TO DO setupVariables(); + defaults
		    
				setupNestedMap(context);
				
		    	setupNormalMap(context);
		    
		    cleanup(context);
	  }
	
	
	
	
}
