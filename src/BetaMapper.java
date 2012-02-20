import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
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

public class BetaMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	Configuration conf = new Configuration();
	Job nestedJob;
	
	Path nestedJobInputPath;
	Path nestedJobOutputPath;
	
	String writerType = null;
	String readerType = null;
	String condition = null;
	
	WriterFactory<KEYIN, VALUEIN> writerFactory = new WriterFactory<KEYIN, VALUEIN>();
	CommonWriterUtils<KEYIN,VALUEIN> writer;
	
	ReaderFactory readerFactory = new ReaderFactory();
	CommonReaderUtils reader;
	
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
		setupNestedJob(nestedJob, conf, condition);
		
		// TODO implement an uniform naming scheme
		nestedJob.setJobName("Layer 2");
		
			setupInternalWR(context);

		// Implement Condition thing;	
		setupNestedMap(context);
			
		setupNestedJob(nestedJob,conf, condition);
		
		setupNesting(nestedJob, conf, condition);
		
		
		while(executeNestedJob() != true){
			// busy wait until nesting is completed
		}
		
		setupNormalMap(context);
		
	}
	

	// TODO Add commented structure
	protected void setupNestedJob(Job job, Configuration conf, String condition) throws IOException{
		setupNesting(nestedJob, conf, condition);
	}
	
	// TODO Add commented structure
	protected void setupNesting(Job job, Configuration conf, String condition ) throws IOException{
		// User provided details for nested job configuration
	}
	
	
	//XXX Fix this fucking bullshit!
	
	private void setupInternalWR(Context context)
		throws IOException, ClassNotFoundException, InstantiationException,
							IllegalAccessException, InterruptedException{
		
		//nestedJobInputPath = writer.getPath();
		nestedJobInputPath = new Path("/tmp/inceptions/" + context.getTaskAttemptID().toString());
		//nestedJobInputPath = new Path("/tmp/outputs/1");
		nestedJobOutputPath = new Path("/tmp/outputs/2");
		
		if(SequenceFileInputFormat.class.getClass() == nestedJob.getInputFormatClass().getClass()){
    		SequenceFileInputFormat.addInputPath(nestedJob, nestedJobInputPath);
    		writerType = "SequenceFile";
		}else{
			writerType = "BufferFile";
		}
    				
		if(SequenceFileOutputFormat.class.getClass() == nestedJob.getOutputFormatClass().getClass()){
    		SequenceFileOutputFormat.setOutputPath(nestedJob, nestedJobOutputPath);
    		readerType = "SequenceFile";
    		
    		System.out.println("I'm in the seqout!");
		}else{
			writerType = "BufferFile";
		}
		
		/*
		if(TextInputFormat.class.getClass() == nestedJob.getInputFormatClass().getClass()){
			FileInputFormat.addInputPath(nestedJob, nestedJobInputPath);
			writerType = "BufferFile";
			
			System.out.println("I'm in the input!");
		}
		
		if(TextOutputFormat.class.getClass() == nestedJob.getOutputFormatClass().getClass()){
			FileOutputFormat.setOutputPath(nestedJob, nestedJobOutputPath);
			System.out.println("I'm in the buffout!");
			readerType = "BufferFile";
		}
		*/
		
	}
	
	@SuppressWarnings("unchecked")
	protected void setupNestedMap(Context context) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException{

		try {
			System.out.println("In the writer and the type is : " + writerType);
			writer =  writerFactory.makeWriter(context, "BufferFile");
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
	
	@SuppressWarnings("unchecked")
	private void setupNormalMap(Context context) throws IOException, InterruptedException {
		
		try{
			System.out.println("In the reader and the reader is of type: " + readerType);
			reader = readerFactory.makeReader(context, "SequenceFile");
		}catch (Exception ex){
			ex.printStackTrace();
		}
		
		while(reader.next()){
			map((KEYIN) reader.getKey(), (VALUEIN)reader.getValue(), context);
		}
		
	}
	
	@Override
	@SuppressWarnings("unchecked")
	protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException{
		context.write((KEYOUT) key, (VALUEOUT) value);
	}
	
	
	

}
