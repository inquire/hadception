package uk.ac.ed.inf.nmr.readers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Class implementing a BufferReader that is used to read the output of the nested job or intermediate file
 * depending on the type that the user provides.
 * @author Daniel Stanoescu
 *
 */

public class NestedReaderBF implements CommonReaderUtils{
	
	/**
	 * {@link SequenceFile} reader instance.
	 */
	
	BufferedReader reader;
	
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
	
	/**
	 * The line that will be processed to find a pair of key/value pairs
	 */
	
	String currentLine;
	
	/**
	 * Store the keys and the values in an array of strings.
	 */
	
	String [] keyValuePair;
	
	/**
	 * Default delimiter between a key/value pair (tab is default)
	 */
	
	String delimiter = "\t";
	
	/**
	 * The type of the key that will be read.
	 */
	
	Text key = new Text();
	
	/**
	 * The value of the key that will be read.
	 */
	
	Text value = new Text();
	
	
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public NestedReaderBF(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException{
		
		conf = context.getConfiguration();
		
		fs = FileSystem.get(URI.create("/tmp/outputs/2/part-r-00000"), conf); 
		path = new Path("/tmp/outputs/2/part-r-00000");
		reader = new BufferedReader(new InputStreamReader(fs.open(path)));
		
	}
	
	/**
	 * Reads the text from the intermediary file or the output of the nested job if one has been started.
	 * @param context Uses the {@link Mapper} context to configure the SequenceFile writer.
	 * @param innerWorks Gets the root directory for the output of the outer job.
	 * @param jobName Uses the name of the job to save the intermediate file so it can be identified by the job that required it.
	 * @param condition Provides the name of the job that it will read from.
	 * @throws IOException
	 */
	
	@SuppressWarnings("rawtypes")
	public NestedReaderBF(org.apache.hadoop.mapreduce.Mapper.Context context,
			Path innerWorks, String jobName, String condition) throws IOException{
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();
		
		if (condition != "default"){ 
			uniqueID = innerWorks + "/outputs/" + jobName + "/" + sequenceOut.toString() + "/" + "/part-r-00000";
		}else{
			uniqueID = innerWorks + "/inceptions/" + jobName + "/" + sequenceOut.toString();

		}
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		reader = new BufferedReader(new InputStreamReader(fs.open(path)));
		
	}

	//XXX Reducer Nested Job will fail!! (caused by current path allocations)
	@Deprecated
	@SuppressWarnings("rawtypes")
	public NestedReaderBF(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException{
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();
		fs = FileSystem.get(URI.create("/tmp/inceptions/" + sequenceOut.toString()), conf);
		path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		
		reader = new BufferedReader(new InputStreamReader(fs.open(path)));
	}
	
	/**
	 * Reads the text from the intermediary file or the output of the nested job if one has been started.
	 * @param context Uses the {@link Reducer} context to configure the SequenceFile writer.
	 * @param innerWorks Gets the root directory for the output of the outer job.
	 * @param jobName Uses the name of the job to save the intermediate file so it can be identified by the job that required it.
	 * @param condition Provides the name of the job that it will read from.
	 * @throws IOException
	 */
	
	@SuppressWarnings("rawtypes")
	public NestedReaderBF(org.apache.hadoop.mapreduce.Reducer.Context context,
			Path innerWorks, String jobName, String condition) throws IOException{
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();

		if (condition != "default"){ 
			uniqueID = innerWorks + "/outputs/" + jobName + "/" + sequenceOut.toString() + "/" + "/part-r-00000";
		}else{
			uniqueID = innerWorks + "/inceptions/" + jobName + "/" + sequenceOut.toString();

		}
		fs = FileSystem.get(URI.create(uniqueID),conf);
		path = new Path(uniqueID);
		
		reader = new BufferedReader(new InputStreamReader(fs.open(path)));	
	}

	/**
	 * @return the reader used to read from the buffer file (text file)
	 */
	
	protected BufferedReader getReader(){
		return reader;
	}
	
	/**
	 * Breaks the string retrieved from the reader into keys and values.
	 */
	
	protected void breakString(){
		keyValuePair = currentLine.split(delimiter);
	}
	
	/**
	 * Returns the key from a key/value pair.
	 */
	
	public Writable getKey(){
		return key;
	}
	
	/**
	 * Returns the value from a key/value pair.
	 */
	
	public Writable getValue(){
		return value;
	}
	
	/**
	 * Ensures that there are more key/value pairs to read.
	 */
	
	public boolean next() throws IOException{
		if((currentLine = reader.readLine()) != null){
			
			breakString();
			key.set(keyValuePair[0]);
			value.set(keyValuePair[1]);
			return true;
			
		}else{
			return false;
		}
		
	}
	
	/**
	 * Allows a delimiter to be set so the read from the text files is efficient. 
	 * @param delimiter the string that delimits the key from a value in a line
	 */
	
	public void setDelimiter(String delimiter){
		this.delimiter = delimiter;
	}
	
	/**
	 * Retrieves the path to the file the reader is reading from.
	 */
	
	@Override
	public Path getPath() {
		return path;
	}
	
	/**
	 * Ensures that the buffer reader is closed. 
	 */
	
	public void close() throws IOException{
		reader.close();
	}


	
}
