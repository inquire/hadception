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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;


public class NestedReaderBF implements CommonReaderUtils{

	BufferedReader reader;
	String currentLine;
	String [] keyValuePair;
	
	LongWritable key = new LongWritable();
	Text value = new Text();
	
	Configuration conf;
	
	@SuppressWarnings("rawtypes")
	public NestedReaderBF(Context context) throws IOException{
		
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
		keyValuePair = currentLine.split(" ");
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
			breakString();
			System.out.println("Printing stuff: " + currentLine);
			key.set(0);
			value.set(keyValuePair[0]);
			return true;
		}else{
			return false;
		}
		
	}
	
	public void close() throws IOException{
		reader.close();
	}
	
}
