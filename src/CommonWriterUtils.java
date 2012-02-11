import java.io.IOException;

public interface CommonWriterUtils {

	public void write(Object key, Object value) throws IOException;
	
	public void close() throws IOException;
}
