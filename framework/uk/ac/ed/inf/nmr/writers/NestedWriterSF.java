package uk.ac.ed.inf.nmr.writers;

import java.net.URI;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;


/**
 * Implementation for a nested writer that uses a SequenceFile as output.
 * It implements the outlines of the {@link CommonWriterUtils} interface.
 * @author Daniel Stanoescu
 */

public class NestedWriterSF implements CommonWriterUtils{

	/**
	 * {@link SequenceFile} writer instance.
	 */
	
	SequenceFile.Writer writer = null;
	
	/**
	 * {@link Configuration} required to setup a {@link SequenceFile}.
	 */
	
	Configuration conf;
	
	/**
	 * {@link FileSystem} support for writing the {@link SequenceFile} to HDFS.
	 */
	
	FileSystem fs;
	
	/**
	 * Contains the path to the written {@link SequenceFile}. 
	 */
	
	Path path;
	
	/**
	 * Creates a unique path id using the jobname and task id.
	 */
	
	String uniqueID;
	
	/**
	 * Creates an intermediate SequenceFile where key/values will be written
	 * @param context Uses the {@link Mapper} context to configure the SequenceFile writer.s
	 * @param innerWorks Gets the root directory for the output of the outer job.
	 * @param jobName Uses the name of the job to save the intermediate file so it can be identified by the job that required it.
	 * @throws Exception
	 */
	
	@SuppressWarnings({ "rawtypes"})
	public NestedWriterSF(org.apache.hadoop.mapreduce.Mapper.Context context, 
			Path innerWorks, String jobName) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  conf = context.getConfiguration();
	  
	  uniqueID = innerWorks + "/inceptions/" + jobName + "/" + mapInput.toString();
	  fs = FileSystem.get(URI.create(uniqueID),conf);
	  path = new Path(uniqueID);
	  FileUtil.chmod(path.toString(), "-rwxr--rwr");
	}
	
	/**
	 * Used to test the writing a file locally. Not used by the framework, here for reference 
	 * @param context Uses the {@link Mapper} context get the configuration necessary for writing a SF.
	 * @throws Exception
	 */
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public NestedWriterSF(org.apache.hadoop.mapreduce.Mapper.Context context) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  conf = context.getConfiguration();
	  
	  fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()),conf);
	  path = new Path("/tmp/inceptions/" + mapInput.toString());
	  
	}
	/**
	 * Creates an intermediate SequenceFile where key/values will be written
	 * @param context Uses the {@link Reducer} context to configure the SequenceFile writer.
	 * @param innerWorks Gets the root directory for the output of the outer job.
	 * @param jobName Uses the name of the job to save the intermediate file so it can be identified by the job that required it.
	 * @throws Exception
	 */
	
	@SuppressWarnings({ "rawtypes"})
	public NestedWriterSF(org.apache.hadoop.mapreduce.Reducer.Context context, 
			Path innerWorks, String jobName) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  conf = context.getConfiguration();

	  uniqueID = innerWorks + "/inceptions/" + jobName + "/" + mapInput.toString();
	  fs = FileSystem.get(URI.create(uniqueID),conf);
	  path = new Path(uniqueID);
	  FileUtil.chmod(path.toString(), "-rwxr--rwr");  
	}
	
	/**
	 * Used to test the writing a file locally. Not used by the framework, here for reference 
	 * @param context Uses the {@link Reducer} context get the configuration necessary for writing a SF.
	 * @throws Exception
	 */
	
	@Deprecated
	@SuppressWarnings({ "rawtypes"})
	public NestedWriterSF(org.apache.hadoop.mapreduce.Reducer.Context context) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  conf = context.getConfiguration();
	  
	  fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()),conf);
	  path = new Path("/tmp/inceptions/" + mapInput.toString());
	}
	
	/**
	 * Provides an implementation for writing a key/value pair in a SequenceFile.
	 * It dynamically shapes the content of the SequenceFile as it sets it's type when it creates the first key value pair.
	 */
	
	@Override
	public void write (Object key, Object value) throws IOException{ 
		if (writer == null){
			
			writer = SequenceFile.createWriter(fs, conf, path,
					key.getClass(), value.getClass());

			writer.append(key, value);
		
		}else{
			writer.append(key, value);
		}
	}
	
	/**
	 * Method for returning the path to the created {@link SequenceFile}
	 */
	
	public Path getPath(){
		return path;
	}
	
	/**
	 * Method closing the stream that was used to write to the SequenceFile
	 */
	@Override
	public void close(){
		  IOUtils.closeStream(writer);	  
	}
}
