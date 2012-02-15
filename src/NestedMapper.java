import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;

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

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;


public class NestedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	
	Configuration conf = new Configuration(); 
	Job nestedJob;
	
	Path nestedJobInputPath;
	Path nestedJobOutputPath;
	
	
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
	  
	  protected void setupNesting(Job job, Configuration conf) throws IOException {
			// TODO Auto-generated method stub

		}  
	  
	  

		protected void setupWorkflow() throws IOException{
			// TODO Auto-generated method stub
	    	
			nestedJob = new Job(conf);
			
			
	    	setupNesting(nestedJob, conf);
	    	
	    	nestedJob.setJobName("Layer 2");
	    	
	    	/*
	    	if(nestedJob == null){
	    		System.err.println("Null nested job details.");
	    	}
			*/

		}

	protected boolean executeNestedJob() 
			throws IOException, ClassNotFoundException, InstantiationException, 
								IllegalAccessException, InterruptedException{
		
		nestedJobInputPath = writer.getPath();
		nestedJobOutputPath = new Path("/tmp/outputs/2");
		
		//if(SequenceFileInputFormat.class.isInstance(nestedJob.getInputFormatClass())){
    		SequenceFileInputFormat.addInputPath(nestedJob, nestedJobInputPath);
    	//}
		
		//if(SequenceFileOutputFormat.class.isInstance(nestedJob.getOutputFormatClass())){
    		SequenceFileOutputFormat.setOutputPath(nestedJob, nestedJobOutputPath);
		//}
		
		if(nestedJob.getInputFormatClass().isInstance(TextInputFormat.class.newInstance())){
			FileInputFormat.addInputPath(nestedJob, nestedJobInputPath);
		}
		
		if(nestedJob.getInputFormatClass().isInstance(TextOutputFormat.class.newInstance())){
			FileOutputFormat.setOutputPath(nestedJob, nestedJobInputPath);
		}
		
		try{
			nestedJob.waitForCompletion(true);
		}catch(ClassNotFoundException ex){
			System.out.println(ex);
		}
		
		return true;
	}
	  
	  
	  
	NestedWriterSF<KEYIN, VALUEIN> writer;

	protected void setupNestedMap(Context context) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException{

		try {
			writer =  new NestedWriterSF<KEYIN, VALUEIN>(context);
		} catch (Exception e) {
			e.printStackTrace();
		}
		  
		  while(context.nextKeyValue()){
			  nestedMap(context.getCurrentKey(), context.getCurrentValue());
		  }
		  
	    writer.close();
		 
	    /* if the user supplies a body for the nested job running run this: */
	    
	    while(executeNestedJob() != true){
	    	// we busy wait for the nested job to finalize;
	    }
	    
		  
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
	  
	  
	  /**
	  protected void runJob(Class<?> map) throws IOException, InterruptedException{
		NestedJobs newJob = new NestedJobs(map, writer.getPath(), new Path("/tmp/outputs/2/"));
		newJob.run();
	}
	
	**/
	  
	  public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    	
		    	setupWorkflow();
		    			    
				try {
					setupNestedMap(context);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		    	setupNormalMap(context);
		    	
		    cleanup(context);
	  }	
}
