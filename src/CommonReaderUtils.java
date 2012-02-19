import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;


public interface CommonReaderUtils {
	
	// XXX add path allocations for reader at instantiation time
	
	public  Writable getKey();
	
	public  Writable getValue();

	public  boolean next() throws IOException;
	
	public Path getPath();
	
	public  void close() throws IOException;
	
}
