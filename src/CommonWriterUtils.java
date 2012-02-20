import java.io.IOException;

import org.apache.hadoop.fs.Path;

public interface CommonWriterUtils {

	public void write(Object key, Object value) throws IOException;
	
	public Path getPath();
	
	public void close() throws IOException;
}
