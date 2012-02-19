import java.io.IOException;

import org.apache.hadoop.fs.Path;

public interface CommonWriterUtils<KEYIN, VALUEIN> {

	public void write(Object key, Object value) throws IOException;
	
	public Path getPath();
	
	public void close() throws IOException;
}
