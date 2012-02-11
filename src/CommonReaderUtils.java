import java.io.IOException;

import org.apache.hadoop.io.Writable;


public interface CommonReaderUtils {

	public Writable getKey();
	
	public Writable getValue();

	public boolean next() throws IOException;
	
	public void close() throws IOException;
	
}
