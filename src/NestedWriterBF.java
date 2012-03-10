

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapreduce.Mapper.Context;

public class NestedWriterBF implements CommonWriterUtils{

	BufferedWriter writer;
	Configuration conf;
	FileSystem fs;
	Path path;
	String uniqueID;
	
	String delimiter = " ";
	
	@SuppressWarnings("rawtypes")
	public NestedWriterBF(org.apache.hadoop.mapreduce.Mapper.Context context,
			Path innerWorks, String jobName) throws Exception{
		
		TaskAttemptID mapInput = context.getTaskAttemptID();  
		conf = context.getConfiguration();
		
		/*
		fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
		path = new Path("/tmp/inceptions/" + mapInput.toString());
		*/
		
		uniqueID = innerWorks + "/inceptions/" + jobName + "/" + mapInput.toString();
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		
		writer = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
	}
	
	@SuppressWarnings("rawtypes")
	public NestedWriterBF(org.apache.hadoop.mapreduce.Mapper.Context context) throws Exception{
		
		TaskAttemptID mapInput = context.getTaskAttemptID();  
		conf = context.getConfiguration();
		
		fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
		path = new Path("/tmp/inceptions/" + mapInput.toString());
	
		writer = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
	}
	
	
	
	@SuppressWarnings("rawtypes")
	public NestedWriterBF(org.apache.hadoop.mapreduce.Reducer.Context context, 
			Path innerWorks, String jobName) throws Exception{
		
		TaskAttemptID mapInput = context.getTaskAttemptID();  
		conf = context.getConfiguration();
		
		/*
		fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
		path = new Path("/tmp/inceptions/" + mapInput.toString());
		*/
		
		uniqueID = innerWorks + "/inceptions/" + jobName + "/" + mapInput.toString();
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		writer = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
	}
	
	@SuppressWarnings("rawtypes")
	public NestedWriterBF(org.apache.hadoop.mapreduce.Reducer.Context context) throws Exception{
		
		TaskAttemptID mapInput = context.getTaskAttemptID();  
		conf = context.getConfiguration();
		
		
		fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
		path = new Path("/tmp/inceptions/" + mapInput.toString());
		

		writer = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
	}
	
	@Override
	public void write(Object key, Object value) throws IOException{
		// TODO Auto-generated method stub
		writer.write(key.toString() + delimiter + value.toString() + "\n");
	}

	/**
	 * Setting a delimiter when writing buffers to HDFS
	 * @param delimiter The type of delimiter necessary between 
	 */
	public void setDelimiter(String delimiter){
		this.delimiter = delimiter;
	}

	@Override
	public Path getPath() {
		// TODO Auto-generated method stub
		return path;
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		writer.close();
	}
}
