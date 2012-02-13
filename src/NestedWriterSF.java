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

public class NestedWriterSF<KEYIN, VALUEIN> implements CommonWriterUtils{

	SequenceFile.Writer writer = null;
	Configuration conf;
	FileSystem fs;
	Path path;
	
	@SuppressWarnings({ "rawtypes"})
	public NestedWriterSF(org.apache.hadoop.mapreduce.Mapper.Context context 
			) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  conf = context.getConfiguration();
	  fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()),conf);
	  path = new Path("/tmp/inceptions/" + mapInput.toString());
  
	}
	
	@SuppressWarnings({ "rawtypes"})
	public NestedWriterSF(org.apache.hadoop.mapreduce.Reducer.Context context 
			) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  conf = context.getConfiguration();
	  fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()),conf);
	  path = new Path("/tmp/inceptions/" + mapInput.toString());
  
	}
	

	@SuppressWarnings({ "rawtypes"})
	public NestedWriterSF(org.apache.hadoop.mapreduce.Mapper.Context context, 
			KEYIN key, VALUEIN value) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  conf = context.getConfiguration();
	  fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()),conf);
	  path = new Path("/tmp/inceptions/" + mapInput.toString());

		    
	}
	
	@SuppressWarnings({ "rawtypes"})
	public NestedWriterSF(org.apache.hadoop.mapreduce.Reducer.Context context,
			KEYIN key, VALUEIN value) throws Exception{
	
	  TaskAttemptID mapInput = context.getTaskAttemptID();  
	  Configuration conf = context.getConfiguration();
	  FileSystem fs = FileSystem.get(URI.create("/tmp/inceptions/" + mapInput.toString()), conf);
	  Path path = new Path("/tmp/inceptions/" + mapInput.toString());

	writer = SequenceFile.createWriter(fs, conf, path, 
				  	key.getClass(),value.getClass());	    
	}
	
	/**
	 * The write method is used in the {@link NestedMapper} to write to a SequenceFile}
	 * @param key The key that will get serialized in the SequenceFile.
	 * @param value The value that will get serialized in the SequenceFile.
	 * @throws IOException
	 */
	
	@Override
	@SuppressWarnings("unchecked")
	public void write (Object key, Object value) throws IOException{ 
		if (writer == null){
			writer = SequenceFile.createWriter(fs, conf, path, 
				  	key.getClass(),value.getClass());
			writer.append((KEYIN) key, (VALUEIN) value);
		}else{
			writer.append((KEYIN) key, (VALUEIN) value);
		}
		//writer.append((KEYIN) key, (VALUEIN) value);
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
