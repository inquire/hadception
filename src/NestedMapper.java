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
	   */
	  
	  // Hacked Implementation 
	  /*
	  protected SequenceFile.Writer setupNesting(Context context) throws IOException, InterruptedException{
		  
		  TaskAttemptID mapInput = context.getTaskAttemptID();
		  
		  Configuration conf = context.getConfiguration();
		  FileSystem fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
		  Path path = new Path("/tmp/inceptions/" + mapInput.toString());
		  SequenceFile.Writer writer = null;
		  
		  writer = SequenceFile.createWriter(fs, conf, path, 
					  						context.getCurrentKey().getClass(),
					  						context.getCurrentValue().getClass());	  
		  return writer;
		  
	  }
	  */
	  
	  protected void setupNesting(Context context) throws IOException, InterruptedException{
		  
		  TaskAttemptID mapInput = context.getTaskAttemptID();
		  
		  Configuration conf = context.getConfiguration();
		  FileSystem fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
		  Path path = new Path("/tmp/inceptions/" + mapInput.toString());
		  SequenceFile.Writer writer = null;
		  
		  //LongWritable key = new LongWritable();
		  //Text value = new Text();
		  
		  System.out.println(context.getMapOutputKeyClass());
		  System.out.println(context.getMapOutputValueClass());
		  
		  
		  try{
			  writer = SequenceFile.createWriter(fs, conf, path, 
					  	context.getMapOutputKeyClass(),
					  	context.getMapOutputValueClass());	  

			  while(context.nextKeyValue()){
				  nestedMap(context.getCurrentKey(), context.getCurrentValue(), writer);
			  }
		  }finally{
			  IOUtils.closeStream(writer);
		  }
 
	  }
	  
	  
	  protected void nestedMap (KEYIN key, VALUEIN value, SequenceFile.Writer writer) throws IOException, InterruptedException{
		  writer.append((KEYIN) key, (VALUEIN) value);
	  }
	  
	  protected void cleanupNesting(SequenceFile.Writer writer){
		  IOUtils.closeStream(writer);
	  }
	  
	  @SuppressWarnings("unchecked")
	  protected void normalMap(Context context) throws IOException, InterruptedException{
		  
		  TaskAttemptID mapOutput = context.getTaskAttemptID();
		  Configuration conf = context.getConfiguration();
		  FileSystem fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapOutput.toString()), conf);
		  Path path = new Path("/tmp/inceptions/" + mapOutput.toString());
		  
		  SequenceFile.Reader reader = null;
		  
		  try{
			  reader = new SequenceFile.Reader(fs, path, conf);
			  
			  Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			  Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			  
			  while(reader.next(key, value)){
				  map((KEYIN) key, (VALUEIN) value, context);
			  }
		  }finally{
			  IOUtils.closeStream(reader);
		  }
	  }
	  
	  public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    
		        setupNesting(context);

		    	normalMap(context);
		    
		    cleanup(context);
		  }
	
	
	
	
}
