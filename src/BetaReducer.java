import java.io.IOException;
	
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class BetaReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
	
	Configuration conf = new Configuration();
	Job nestedJob;
	
	Path nestedJobInputPath;
	Path nestedJobOutputPath;
	
	Path innerWorks = new Path("/tmp/");
	
	String writerType = null;
	String readerType = null;
	String condition = null;
	
	WriterFactory writerFactory = new WriterFactory();
	CommonWriterUtils writer;
	
	ReaderFactory readerFactory = new ReaderFactory();
	CommonReaderUtils reader;
	
	//TODO Add commented structure
	
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
		
		System.out.println(innerWorks);
		
		//conf.set("hadoop.job.ugi", context.getConfiguration().getResource("hadoop.job.ugi").toString());
		
		nestedJob = new Job(conf);
		
		setupNestedJob(nestedJob, conf, condition);
		
		// TODO implement an uniform naming scheme
		nestedJob.setJobName("Layer-2");
		
			setupInternalWR(context);
		
		condition = "something";

		// Implement Condition thing;	
		setupNestedReducer(context);
			
		setupNestedJob(nestedJob,conf, condition);
		
		setupNesting(nestedJob, conf, condition);
		
		while(executeNestedJob() != true){
			// busy wait until nesting is completed
		}
		
		setupNormalReducer(context);
		
	}
	
	// TODO Add commented structure
	protected void setupNestedJob(Job job, Configuration conf, String condition) throws IOException{
		setupNesting(nestedJob, conf, condition);
	}
	
	// TODO Add commented structure
	protected void setupNesting(Job job, Configuration conf, String condition ) throws IOException{
		// User provided details for nested job configuration
	}
	
	private void setupInternalWR(Context context)
		throws IOException, ClassNotFoundException, InstantiationException,
							IllegalAccessException, InterruptedException{

		//nestedJobInputPath = writer.getPath();
		nestedJobInputPath = new Path(innerWorks + "/inceptions/" + nestedJob.getJobName() + "/" + context.getTaskAttemptID().toString());
		//nestedJobInputPath = new Path("/tmp/outputs/1");
		//nestedJobOutputPath = new Path("/tmp/outputs/2");
		nestedJobOutputPath = new Path(innerWorks + "/outputs/" + nestedJob.getJobName() + "/" + context.getTaskAttemptID());


		//SequenceFileInputFormat.addInputPath(nestedJob, nestedJobInputPath);
		//SequenceFileOutputFormat.setOutputPath(nestedJob, nestedJobOutputPath);


		if(SequenceFileInputFormat.class.getName() == nestedJob.getInputFormatClass().getName()){
			SequenceFileInputFormat.addInputPath(nestedJob, nestedJobInputPath);
			writerType = "SequenceFile";
		}

		if(SequenceFileOutputFormat.class.getName() == nestedJob.getOutputFormatClass().getName()){
			SequenceFileOutputFormat.setOutputPath(nestedJob, nestedJobOutputPath);
			readerType = "SequenceFile";
		}


		if(TextInputFormat.class.getName() == nestedJob.getInputFormatClass().getName()){
			FileInputFormat.addInputPath(nestedJob, nestedJobInputPath);
			writerType = "BufferFile";
		}

		if(TextOutputFormat.class.getName() == nestedJob.getOutputFormatClass().getName()){
			FileOutputFormat.setOutputPath(nestedJob, nestedJobOutputPath);
			readerType = "BufferFile";
		}
			
			
	}
	
	protected void setupNestedReducer(Context context) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException{

		try {
			System.out.println("In the writer and the type is : " + writerType);
			writer = writerFactory.makeWriter(context, innerWorks, nestedJob.getJobName(), writerType);
			//writer = new NestedWriterSF<KEYIN, VALUEIN>(context, innerWorks, nestedJob.getJobName());
			//writer = writerFactory.makeWriter(context, writerType);
		} catch (Exception e) {
			e.printStackTrace();
		}
		  
		  while(context.nextKeyValue()){
			  nestedReducer(context.getCurrentKey(), context.getValues(), condition);
		  }
		  
	    writer.close();
		 
	    /* if the user supplies a body for the nested job running run this: */
	    
	 //   while(executeNestedJob() != true){
	    	// we busy wait for the nested job to finalize;
	 //   }
	}
	
	protected void nestedReducer (KEYIN key, Iterable<VALUEIN> value, String condition) throws IOException, InterruptedException{
		System.out.println(key + " / " + value);
	
		writer.write(key, value);
	}
	
	protected boolean executeNestedJob(){
	
			try{
				nestedJob.waitForCompletion(true);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
		return true;
	}
	
	private void setupNormalReducer(Context context) throws IOException, InterruptedException {

		try{
			System.out.println("In the reader and the reader is of type: " + readerType);
			//reader = readerFactory.makeReader(context, readerType);
			reader = readerFactory.makeReader(context, innerWorks, nestedJob.getJobName(), readerType, condition);
		}catch (Exception ex){
			ex.printStackTrace();
		}

		while(reader.next()){
			reduce(reader.getKey(), reader.getValue(), context);
		}

	}
	
	@SuppressWarnings("unchecked")
	protected void reduce(Writable key, Writable value, Context context) throws IOException, InterruptedException{
		
		context.write((KEYOUT) key, (VALUEOUT) value);
	}
	
	



}
