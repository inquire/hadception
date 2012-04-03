package uk.ac.ed.inf.nmr.writers;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 * An interface for standardizing current and future methods of writers used with the framework.
 * @author Daniel Stanoescu
 *
 */

public interface CommonWriterUtils {

	/**
	 * Writes a key/value pair to the a file or a buffer.
	 * @param key the key that will be written
	 * @param value the value that will be written
	 * @throws IOException
	 */
	public void write(Object key, Object value) throws IOException;
	
	public Path getPath();
	
	/**
	 * Ensures that after everything has been written the buffer is closed.
	 * @throws IOException
	 */
	public void close() throws IOException;
}
