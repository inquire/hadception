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
	
	Writable key = null;
	Writable value = null;
	
	
	
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Mapper.Context context) 
			throws IOException{
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();
		fs = FileSystem.get(URI.create("/tmp/inceptions/" + sequenceOut.toString()), conf);
		//path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		path = new Path("/tmp/outputs/2/part-r-00000");
		reader = new SequenceFile.Reader(fs, path, conf);
		
		key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		  
	}
	
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Reducer.Context context) 
			throws IOException{
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();
		fs = FileSystem.get(URI.create("/tmp/inceptions/" + sequenceOut.toString()), conf);
		path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		
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
