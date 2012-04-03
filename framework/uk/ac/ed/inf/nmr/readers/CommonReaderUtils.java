package uk.ac.ed.inf.nmr.readers;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

/**
 * An interface for standardizing current and future methods of readers used with the framework.
 * @author Daniel Stanoescu
 *
 */

public interface CommonReaderUtils {
	
	/**
	 * A method that will return a key from the location it reads
	 * @return a key
	 */
	public Writable getKey();
	
	/**
	 * A method that will return a value from the location it reads
	 * @return a value
	 */
	public Writable getValue();

	/**
	 * Checks if there are any more pairs of key/values to return.
	 * @return True if there is still input to read.
	 * @throws IOException
	 */
	public boolean next() throws IOException;
	
	public Path getPath();
	
	/**
	 * Ensures the buffer closes after everything has been read. 
	 * @throws IOException
	 */
	public void close() throws IOException;
	
}
