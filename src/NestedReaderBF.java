import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptID;


public class NestedReaderBF implements CommonReaderUtils{

	BufferedReader reader;
	String currentLine;
	String [] keyValuePair;
	String delimiter = " ";
	
	//XXX fix the variable instantiation (add reflection or something)
	
	LongWritable key = new LongWritable();
	Text value = new Text();
	
	Configuration conf;
	
	@SuppressWarnings("rawtypes")
	public NestedReaderBF(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException{
		
		//FIXME automagically path allocation
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();
		FileSystem fs = FileSystem.get(URI.create("/tmp/inceptions/" + sequenceOut.toString()), conf);
		Path path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		
		reader = new BufferedReader(new InputStreamReader(fs.open(path)));
		//key = (Text) ReflectionUtils.newInstance(Text.class, conf);
		//value = (Text) ReflectionUtils.newInstance(Text.class, conf);
		
	}
	
	@SuppressWarnings("rawtypes")
	public NestedReaderBF(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException{
		
		//FIXME automagically path allocation
		
		conf = context.getConfiguration();
		TaskAttemptID sequenceOut = context.getTaskAttemptID();
		FileSystem fs = FileSystem.get(URI.create("/tmp/inceptions/" + sequenceOut.toString()), conf);
		Path path = new Path("/tmp/inceptions/" + sequenceOut.toString());
		
		reader = new BufferedReader(new InputStreamReader(fs.open(path)));
		//key = (Text) ReflectionUtils.newInstance(Text.class, conf);
		//value = (Text) ReflectionUtils.newInstance(Text.class, conf);
		
	}
	
	

	protected BufferedReader getReader(){
		return reader;
	}
	
	protected void breakString(){
		keyValuePair = currentLine.split(delimiter);
	}
	
	public Writable getKey(){
		//key.set(keyValuePair[0]);
		return key;
	}
	
	public Writable getValue(){
		//value.set(keyValuePair[0]);
		return value;
	}
	
	public boolean next() throws IOException{
		if((currentLine = reader.readLine()) != null){
			
			// TO DO: need to refine buffer reader
			
			breakString();
			System.out.println("Printing stuff: " + currentLine);
			key.set(Long.valueOf( keyValuePair[0]));
			System.out.println("Key is: " + key);
			value.set(currentLine.substring(keyValuePair[0].length()));
			System.out.println("Value is: " + value);

			return true;
		}else{
			return false;
		}
		
	}
	
	public void setDelimiter(String delimiter){
		this.delimiter = delimiter;
	}
	
	public void close() throws IOException{
		reader.close();
	}
	
}
