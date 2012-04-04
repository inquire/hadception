package uk.ac.ed.inf.nmr.writers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

/**
 * Class implementing a BufferReader that is write a BufferFile it the join input of the nested job is a plain text file.
 * @author Daniel Stanoescu
 *
 */

public class NestedWriterBF implements CommonWriterUtils{

	/**
	 * {@link SequenceFile} reader instance.
	 */
	
	BufferedWriter writer;
	
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
	 * Write the keys and values using a delimiter.
	 */
	
	String delimiter = "\t";
	
	/**
	 * Writes a buffer file (plain text) as an intermediate file on HDFS.
	 * @param context The {@link Mapper} context required to write a file.
	 * @param innerWorks The root directory where the file will be written to.
	 * @param jobName Used to write the file in a folder where all similar intermediate files created by a similar job reside.
	 * @throws Exception
	 */
	
	@SuppressWarnings("rawtypes")
	public NestedWriterBF(org.apache.hadoop.mapreduce.Mapper.Context context,
			Path innerWorks, String jobName) throws Exception{
		
		TaskAttemptID mapInput = context.getTaskAttemptID();  
		conf = context.getConfiguration();
		
		uniqueID = innerWorks + "/inceptions/" + jobName + "/" + mapInput.toString();
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		
		writer = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
	}
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public NestedWriterBF(org.apache.hadoop.mapreduce.Mapper.Context context) throws Exception{
		
		TaskAttemptID mapInput = context.getTaskAttemptID();  
		conf = context.getConfiguration();
		
		fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
		path = new Path("/tmp/inceptions/" + mapInput.toString());
	
		writer = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
	}
	
	/**
	 * Writes a buffer file (plain text) as an intermediate file on HDFS.
	 * @param context The {@link Reducer} context required to write a file.
	 * @param innerWorks The root directory where the file will be written to.
	 * @param jobName Used to write the file in a folder where all similar intermediate files created by a similar job reside.
	 * @throws Exception
	 */
	
	@SuppressWarnings("rawtypes")
	public NestedWriterBF(org.apache.hadoop.mapreduce.Reducer.Context context, 
			Path innerWorks, String jobName) throws Exception{
		
		TaskAttemptID mapInput = context.getTaskAttemptID();  
		conf = context.getConfiguration();
		
		uniqueID = innerWorks + "/inceptions/" + jobName + "/" + mapInput.toString();
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		writer = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
	}
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public NestedWriterBF(org.apache.hadoop.mapreduce.Reducer.Context context) throws Exception{
		
		TaskAttemptID mapInput = context.getTaskAttemptID();  
		conf = context.getConfiguration();
			
		fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
		path = new Path("/tmp/inceptions/" + mapInput.toString());
		
		writer = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
	}
	
	/**
	 * Write a key/value pair to a plain text file on HDFS using the {@link delimiter}
	 */
	
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

	/**
	 * Retrieves the path to the file the reader is reading from.
	 */
	
	@Override
	public Path getPath() {
		// TODO Auto-generated method stub
		return path;
	}
	
	/**
	 * Ensure that after writing the file gets closed.
	 */
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		writer.close();
	}
}
