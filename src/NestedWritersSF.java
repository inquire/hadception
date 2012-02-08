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
import org.apache.hadoop.util.GenericsUtil;


public class NestedWritersSF<KEYIN, VALUEIN> {

	//@SuppressWarnings("rawtypes")
	//Context context;
	
	//@SuppressWarnings("rawtypes")
	//public void setDelegatedContext(Context delegatedContext){
	//	context = delegatedContext;
	//}

	
	SequenceFile.Writer writer;
	
	@SuppressWarnings({ "rawtypes"})
	public NestedWritersSF(Context delcontext, KEYIN key, VALUEIN value) throws Exception{
	
	Context context = delcontext;
	TaskAttemptID mapInput = context.getTaskAttemptID();
	  
	  Configuration conf = context.getConfiguration();
	  FileSystem fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
	  Path path = new Path("/tmp/inceptions/" + mapInput.toString());
	  
	  
	  //SequenceFile.Writer writer = null;
		   
	 // try{
		  writer = SequenceFile.createWriter(fs, conf, path, 
				  	key.getClass(),
				  	value.getClass());	  

		//  while(context.nextKeyValue()){
		 // write(context.getCurrentKey(), context.getCurrentValue());
		//  }
	 // }finally{
		  //IOUtils.closeStream(writer);
	 // }
}
	@SuppressWarnings("unchecked")
	protected void write (Object object, Object object2) throws IOException{ 
		System.out.println("in me");
		writer.append((KEYIN) object, (VALUEIN) object2);
	}
	
	protected void close(){
		  IOUtils.closeStream(writer);

	}
}
