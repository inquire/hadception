import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BetaMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	Configuration conf = new Configuration();
	Job nestedJob;
	
	Path nestedJobInputPath;
	Path nestedJobOutputPath;
	
	String writerType = null;
	String readerType = null;
	String condition = null;
	
	
	// TODO Add commented structure
	@Override
	public void run(Context context) throws IOException, InterruptedException{
		
		setup(context);
			try {
				setupWorkflow(context);
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
		cleanup(context);
	}

	// TODO Add commented structure
	private void setupWorkflow(Context context) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		nestedJob = new Job(conf);
		setupNestedJob(nestedJob, condition);
		
		// TODO implement an uniform naming scheme
		nestedJob.setJobName("Layer 2");
		
			setupInternalWR();

		// Implement Condition thing;	
		setupNestedMap(context);
			
		setupNestedJob(nestedJob,condition);
		
		
		

	}

	// TODO Add commented structure
	protected void setupNestedJob(Job job, String condition) throws IOException{
		setupNesting(nestedJob, condition);
	}
	
	// TODO Add commented structure
	protected void setupNesting(Job job, String condition ){
		// User provided details for nested job configuration
	}
	
	
	private void setupInternalWR()
		throws IOException, ClassNotFoundException, InstantiationException,
							IllegalAccessException, InterruptedException{
		
		//nestedJobInputPath = writer.getPath();
		nestedJobInputPath = new Path("/tmp/outputs/1");
		nestedJobOutputPath = new Path("/tmp/outputs/2");
		
		if(SequenceFileInputFormat.class.getClass() == nestedJob.getInputFormatClass().getClass()){
    		SequenceFileInputFormat.addInputPath(nestedJob, nestedJobInputPath);
    		writerType = "SequenceFile";
		}
    				
		if(SequenceFileOutputFormat.class.getClass() == nestedJob.getOutputFormatClass().getClass()){
    		SequenceFileOutputFormat.setOutputPath(nestedJob, nestedJobOutputPath);
    		readerType = "SequenceFile";
		}
		
		if(TextInputFormat.class.getClass() == nestedJob.getInputFormatClass().getClass()){
			FileInputFormat.addInputPath(nestedJob, nestedJobInputPath);
			writerType = "BufferFile";
		}
		
		if(TextOutputFormat.class.getClass() == nestedJob.getOutputFormatClass().getClass()){
			FileOutputFormat.setOutputPath(nestedJob, nestedJobInputPath);
			readerType = "BufferFile";
		}
		
	}
	CommonWriterUtils writer;

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
	    
	 //   while(executeNestedJob() != true){
	    	// we busy wait for the nested job to finalize;
	 //   }
	}
	  protected void nestedMap (KEYIN key, VALUEIN value) throws IOException, InterruptedException{
		System.out.println(key + " / " + value);
	
		writer.write((KEYIN) key, (VALUEIN) value);
	  }
	
	
	
	
	
	
	
	

}
