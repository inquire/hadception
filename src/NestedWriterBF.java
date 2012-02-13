import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapreduce.Mapper.Context;

public class NestedWriterBF<KEYIN, VALUEIN> implements CommonWriterUtils{

	BufferedWriter writer;
	String delimiter = " ";
	
	@SuppressWarnings("rawtypes")
	public NestedWriterBF(org.apache.hadoop.mapreduce.Mapper.Context context, KEYIN key, VALUEIN value) throws Exception{
		
		TaskAttemptID mapInput = context.getTaskAttemptID();  
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
		Path path = new Path("/tmp/inceptions/" + mapInput.toString());
		
		writer = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
	}
	
	@SuppressWarnings("rawtypes")
	public NestedWriterBF(org.apache.hadoop.mapreduce.Reducer.Context context, KEYIN key, VALUEIN value) throws Exception{
		
		TaskAttemptID mapInput = context.getTaskAttemptID();  
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
		Path path = new Path("/tmp/inceptions/" + mapInput.toString());
		
		writer = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
	}
	
	@Override
	public void write(Object key, Object value) throws IOException{
		// TODO Auto-generated method stub
		writer.write(key.toString() + delimiter + value.toString() + "\n");
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		writer.close();
	}
	
	/**
	 * Setting a delimiter when writing buffers to HDFS
	 * @param delimiter The type of delimiter necessary between 
	 */
	public void setDelimiter(String delimiter){
		this.delimiter = delimiter;
	}
	
	
}
