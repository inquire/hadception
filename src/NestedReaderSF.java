

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

public class NestedReaderSF implements CommonReaderUtils{

	SequenceFile.Reader reader;
	Configuration conf;
	FileSystem fs;
	Path path;
	String uniqueID;
	
	Writable key = null;
	Writable value = null;
	
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Mapper.Context context) 
			throws IOException{
		
		//FIXME automagically path allocation;
		
		conf = context.getConfiguration();
		//TaskAttemptID sequenceOut = context.getTaskAttemptID();
		fs = FileSystem.get(URI.create("/tmp/outputs/2/part-r-00000"), conf);
		//path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		path = new Path("/tmp/outputs/2/part-r-00000");
		
		reader = new SequenceFile.Reader(fs, path, conf);
		
		key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		  
	}
	
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Mapper.Context context, 
			Path innerWorks, String jobName, String condition) 
			throws IOException{
		
		//FIXME automagically path allocation;
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();
		
		/*
		fs = FileSystem.get(URI.create("/tmp/outputs/2/part-r-00000"), conf);
		//path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		path = new Path("/tmp/outputs/2/part-r-00000");
		*/
		if (condition != "default"){ 
			uniqueID = innerWorks + "/outputs/" + jobName + "/" + sequenceOut.toString() + "/" + "/part-r-00000";
		}else{
			uniqueID = innerWorks + "/inceptions/" + jobName + "/" + sequenceOut.toString();

		}
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		
		reader = new SequenceFile.Reader(fs, path, conf);
		
		key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		  
	}
	
	//XXX Reducer Nested Job will fail!! (caused by current path allocations)
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Reducer.Context context) 
			throws IOException{
		
		//FIXME automagically path allocation;
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();
		fs = FileSystem.get(URI.create("/tmp/outputs/2/part-r-00000"), conf);
		//path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		
		reader = new SequenceFile.Reader(fs, path, conf);
		
		key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		  
	}
	
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Reducer.Context context,
			Path innerWorks, String jobName, String condition) throws IOException{
		
		//FIXME automagically path allocation;
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();
		
		/*
		fs = FileSystem.get(URI.create("/tmp/outputs/2/part-r-00000"), conf);
		//path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		*/
		
		System.out.println(condition);
		
		if(condition != "default"){
			uniqueID = innerWorks + "/outputs/" + jobName + "/" + sequenceOut.toString() + "/" + "/part-r-00000";
		}else{
			uniqueID = innerWorks + "/inceptions/" + jobName + "/" + sequenceOut.toString();
		}
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		
		reader = new SequenceFile.Reader(fs, path, conf);
		
		key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		  
	}
	
	
	// ====================== Alternative =================================
	
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Mapper.Context context, 
			String agreggatePath, String condition) 
			throws IOException{
		
		//FIXME automagically path allocation;
		
		conf = context.getConfiguration();
		//TaskAttemptID sequenceOut = context.getTaskAttemptID();
		
		/*
		fs = FileSystem.get(URI.create("/tmp/outputs/2/part-r-00000"), conf);
		//path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		path = new Path("/tmp/outputs/2/part-r-00000");
		*/
		if (condition != null){ 
			//uniqueID = innerWorks + "/outputs/" + jobName + "/" + sequenceOut.toString() + "/" + "/part-r-00000";
			uniqueID = agreggatePath;
		}else{
			//uniqueID = innerWorks + "/inceptions/" + jobName + "/" + sequenceOut.toString();

		}
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		reader = new SequenceFile.Reader(fs, path, conf);
		
		key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
	}
	
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Reducer.Context context, 
			String agreggatePath, String condition) 
			throws IOException{
		
		//FIXME automagically path allocation;
		
		conf = context.getConfiguration();
		//TaskAttemptID sequenceOut = context.getTaskAttemptID();
		
		/*
		fs = FileSystem.get(URI.create("/tmp/outputs/2/part-r-00000"), conf);
		//path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		path = new Path("/tmp/outputs/2/part-r-00000");
		*/
		if (condition != null){ 
			//uniqueID = innerWorks + "/outputs/" + jobName + "/" + sequenceOut.toString() + "/" + "/part-r-00000";
			uniqueID = agreggatePath;
		}else{
			//uniqueID = innerWorks + "/inceptions/" + jobName + "/" + sequenceOut.toString();

		}
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		reader = new SequenceFile.Reader(fs, path, conf);
		
		key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
	}
	
	protected SequenceFile.Reader getReader(){
		return reader;
	}
	
	public Writable getKey(){
 		return key;
	}
	
	public Writable getValue(){
		return value;
	}
	
	public boolean next() throws IOException{
		if (reader.next(key, value)){
			return true;
		}else{
			return false;
		}
	}
	
	public Path getPath(){
		return path;
	}
	
	
	public void close() throws IOException{
		IOUtils.closeStream(reader);
	}
	
}
