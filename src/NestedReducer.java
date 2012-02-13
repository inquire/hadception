import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;


public class NestedReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{


  /**
   * Called once at the start of the task.
   * It contains: 
   * 
   * conf, taskid, RawKeyValueIterator, 
   * Counter KeyCounter/ValueCounter,
   * RecordWriter, OutputCommiter, 
   * StatusReporter, RawComparator, 
   * keyClass, valueClass
   * 
   */
  protected void setup(Context context
                       ) throws IOException, InterruptedException {
    // NOTHING

  }

  /**
   * This method is called once for each key. Most applications will define
   * their reduce class by overriding this method. The default implementation
   * is an identity function.
   */
  @SuppressWarnings("unchecked")
  protected void reduce(KEYIN key, VALUEIN value, Context context
                        ) throws IOException, InterruptedException {

      context.write((KEYOUT) key, (VALUEOUT) value);

  }

  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
    // NOTHING
  }

  NestedWriterSF<KEYIN, VALUEIN> writer;
  private void setupNestedReducer(Context context) throws IOException, InterruptedException {
  
	try {
		writer =  new NestedWriterSF<KEYIN, VALUEIN>(context);
	} catch (Exception e) {
		e.printStackTrace();
	}
	  
	  while(context.nextKeyValue()){
		  nestedReducer(context.getCurrentKey(), context.getValues());
	  }
	  
	  writer.close();
	  
	  
	}
  
  @SuppressWarnings("unchecked")
  protected void nestedReducer(KEYIN key, Iterable<VALUEIN> values) throws IOException, InterruptedException{
	  
	  for (VALUEIN value : values ){
		  writer.write((KEYOUT) key, (VALUEOUT) value);
	  }
  }
  
  
  
  NestedReaderSF reader;
  @SuppressWarnings("unchecked")
  private void setupNormalReducer(Context context) throws IOException, InterruptedException {
	  reader = new NestedReaderSF(context);
	  
	  while (reader.next()){
		  reduce((KEYIN) reader.getKey(), (VALUEIN) reader.getValue(), context);		
	}
  }

  /**
   * Advanced application writers can use the 
   * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
   * control how the reduce task works.
   */
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    
    // TO DO setupVariables(); + defaults
    
    	setupNestedReducer(context);
    	
    	setupNormalReducer(context);
    
    cleanup(context);
  }


}
