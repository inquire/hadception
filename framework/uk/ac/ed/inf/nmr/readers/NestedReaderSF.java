package uk.ac.ed.inf.nmr.readers;

import java.net.URI;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

public class NestedReaderSF implements CommonReaderUtils{

	/**
	 * {@link SequenceFile} reader instance.
	 */
	
	SequenceFile.Reader reader;
	
	/**
	 * {@link Configuration} required to setup a {@link SequenceFile}.
	 */
	
	Configuration conf;
	
	/**
	 * {@link FileSystem} support for reading the {@link SequenceFile} from HDFS.
	 */
	
	FileSystem fs;
	
	/**
	 * Contains the path to the {@link SequenceFile} that it will read.
	 */
	
	Path path;
	
	/**
	 * Creates a unique path id using the jobname and task id.
	 */
	
	String uniqueID;
	
	/**
	 * Initializes the key that will be read from the {@link SequenceFile}.
	 */
	
	Writable key = null;
	
	/**
	 * Initializes the value that will be read from the {@link SequenceFile}.
	 */
	
	Writable value = null;
	
	
	/**
	 * Used to test the reading of a file locally. Not used by the framework, here for reference 
	 * @param context Uses the {@link Mapper} context get the configuration necessary for writing a SF.
	 * @throws Exception
	 */
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Mapper.Context context) 
			throws IOException{
		
		conf = context.getConfiguration();
		fs = FileSystem.get(URI.create("/tmp/outputs/2/part-r-00000"), conf);
		path = new Path("/tmp/outputs/2/part-r-00000");
		
		reader = new SequenceFile.Reader(fs, path, conf);
		
		key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		  
	}
	
	/**
	 * Used to read key/value pairs from a {@link SequenceFile}.
	 * @param context Uses the {@link Mapper} context to configure the SequenceFile writer.
	 * @param innerWorks Gets the root directory for the output of the outer job.
	 * @param jobName Uses the name of the job to save the intermediate file so it can be identified by the job that required it.
	 * @param condition Provides the name of the job that it will read from.
	 * @throws IOException
	 */
	
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Mapper.Context context, 
			Path innerWorks, String jobName, String condition) 
			throws IOException{
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();

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
	
	/**
	 * Used to test the reading of a file locally. Not used by the framework, here for reference 
	 * @param context Uses the {@link Reducer} context get the configuration necessary for writing a SF.
	 * @throws Exception
	 */
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Reducer.Context context) 
			throws IOException{

		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();
		fs = FileSystem.get(URI.create("/tmp/outputs/2/part-r-00000"), conf);
		path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		
		reader = new SequenceFile.Reader(fs, path, conf);
		
		key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		  
	}
	
	/**
	 * Used to read key/value pairs from a {@link SequenceFile}.
	 * @param context Uses the {@link Reducer} context to configure the SequenceFile writer.
	 * @param innerWorks Gets the root directory for the output of the outer job.
	 * @param jobName Uses the name of the job to save the intermediate file so it can be identified by the job that required it.
	 * @param condition Provides the name of the job that it will read from.
	 * @throws IOException
	 */
	
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Reducer.Context context,
			Path innerWorks, String jobName, String condition) throws IOException{
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();

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
	
	/**
	 * The current reader that aggregates the output of the nested job and reads from the created files.
	 * @param context The {@link Mapper} context required to read from a SequenceFile.
	 * @param agreggatePath The path to the location of the output of the nested job.
	 * @param condition Identifier to locate the files that are required for reading.
	 * @throws IOException
	 */
	
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Mapper.Context context, 
			String agreggatePath, String condition) 
			throws IOException{
		
		conf = context.getConfiguration();

		if (condition != null){ 
			uniqueID = agreggatePath;
		}else{
			//there is no else.
		}
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		reader = new SequenceFile.Reader(fs, path, conf);
		
		key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
	}
	
	/**
	 * The current reader that aggregates the output of the nested job and reads from the created files.
	 * @param context The {@link Reducer} context required to read from a SequenceFile.
	 * @param agreggatePath The path to the location of the output of the nested job.
	 * @param condition Identifier to locate the files that are required for reading.
	 * @throws IOException
	 */
	
	@SuppressWarnings("rawtypes")
	public NestedReaderSF(org.apache.hadoop.mapreduce.Reducer.Context context, 
			String agreggatePath, String condition) 
			throws IOException{

		conf = context.getConfiguration();

		if (condition != null){ 
			uniqueID = agreggatePath;
		}else{
			//there is no else.
		}
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		reader = new SequenceFile.Reader(fs, path, conf);
		
		key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
	}
	
	/**
	 * @return The {@link SequenceFile} reader used for the reading.
	 */
	
	protected SequenceFile.Reader getReader(){
		return reader;
	}
	
	/**
	 * Returns the key of a key/value pair.
	 */
	
	public Writable getKey(){
 		return key;
	}
	
	/**
	 * Returns the value of a key/value pair.
	 */
	
	public Writable getValue(){
		return value;
	}
	
	/**
	 * Ensures that there are still more key/value pairs to read.
	 */
	
	public boolean next() throws IOException{
		if (reader.next(key, value)){
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * Returns the path of the file it reads from.
	 */
	
	public Path getPath(){
		return path;
	}
	
	/**
	 * Ensures that the file is closed after reading.
	 */
	
	public void close() throws IOException{
		IOUtils.closeStream(reader);
	}
	
}
