import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.io.SequenceFile.Writer;


/**
 * Implementation for a nested writer that uses a SequenceFile as output.
 * It is meant to create an intermediate value to be used in a following nesting stage.
 * @author Jack
 *
 * @param <KEYIN> The class type of the key that gets serialized in the SequenceFile.
 * @param <VALUEIN> The class type of the value that gets serialized in the SequenceFile.
 */

public class NestedWriterSF implements CommonWriterUtils{

	SequenceFile.Writer writer = null;
	Configuration conf;
	FileSystem fs;
	Path path;
	String uniqueID;
	
	@SuppressWarnings({ "rawtypes"})
	public NestedWriterSF(org.apache.hadoop.mapreduce.Mapper.Context context, 
			Path innerWorks, String jobName) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  conf = context.getConfiguration();
	  
	  /*
	  fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()),conf);
	  path = new Path("/tmp/inceptions/" + mapInput.toString());
	  */
	  
	  uniqueID = innerWorks + "/inceptions/" + jobName + "/" + mapInput.toString();
	  System.out.println(uniqueID);
	  fs = FileSystem.get(URI.create(uniqueID),conf);
	  path = new Path(uniqueID);
  
	}
	
	@SuppressWarnings("rawtypes")
	public NestedWriterSF(org.apache.hadoop.mapreduce.Mapper.Context context) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  conf = context.getConfiguration();
	  
	  fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()),conf);
	  path = new Path("/tmp/inceptions/" + mapInput.toString());
	  
	}
	
	@SuppressWarnings({ "rawtypes"})
	public NestedWriterSF(org.apache.hadoop.mapreduce.Reducer.Context context, 
			Path innerWorks, String jobName) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  conf = context.getConfiguration();
	  
	  /*
	  fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()),conf);
	  path = new Path("/tmp/inceptions/" + mapInput.toString());
	  */
	  
	  uniqueID = innerWorks + "/inceptions/" + jobName + "/" + mapInput.toString();
	  fs = FileSystem.get(URI.create(uniqueID),conf);
	  path = new Path(uniqueID);
	  
	}
	
	@SuppressWarnings({ "rawtypes"})
	public NestedWriterSF(org.apache.hadoop.mapreduce.Reducer.Context context) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  conf = context.getConfiguration();
	  
	  fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()),conf);
	  path = new Path("/tmp/inceptions/" + mapInput.toString());
	 
	}
	
	@Override
	public void write (Object key, Object value) throws IOException{ 
		if (writer == null){
			
			System.out.println(key.getClass().getName());
			System.out.println(value.getClass().getName());
			
			writer = SequenceFile.createWriter(fs, conf, path, 
				  	key.getClass(),value.getClass());
			
			writer.append(key, value);
		}else{
			writer.append(key, value);
		}
		
		System.out.println("Cica intra aici");

	}
	
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
