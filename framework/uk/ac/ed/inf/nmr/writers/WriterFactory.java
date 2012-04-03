package uk.ac.ed.inf.nmr.writers;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import uk.ac.ed.inf.nmr.mapreduce.NestedMapper;
import uk.ac.ed.inf.nmr.mapreduce.NestedReducer;

/**
 * A writer factory which creates a writer based on the type of input a nested job requires.
 * @author Daniel Stanoescu
 *
 */

public class WriterFactory {
	
	
	// ========================= Factories with custom writer path ================================
	
	/**
	 * Makes a writer for the {@link NestedMapper} to create intermediary files.
	 * @param context Uses the {@link Context} of the Hadoop {@link Mapper}.
	 * @param innerWorks Requires a path where to write the file it creates.
	 * @param jobName Uses a jobName to construct the path to the file.
	 * @param writerType Creates the appropriate writer for the file it requires
	 * @return Sanity check
	 * @throws Exception
	 */
	
	@SuppressWarnings("rawtypes")
	public CommonWriterUtils makeWriter(org.apache.hadoop.mapreduce.Mapper.Context context, 
			Path innerWorks, String jobName, String writerType) throws Exception{

		if (writerType == "SequenceFile"){
			return new NestedWriterSF(context, innerWorks, jobName);
		}
		
		if (writerType == "BufferFile"){
			return new NestedWriterBF(context, innerWorks, jobName);
		}
		
		return null;
	}
	
	/**
	 * Makes a writer for the {@link NestedReducer} to create intermediary files.
	 * @param context Uses the {@link Context} of the Hadoop {@link Reducer}.
	 * @param innerWorks Requires a path where to write the file it creates.
	 * @param jobName Uses a jobName to construct the path to the file.
	 * @param writerType Creates the appropriate writer for the file it requires
	 * @return Sanity check
	 * @throws Exception
	 */
	
	@SuppressWarnings("rawtypes")
	public CommonWriterUtils makeWriter(org.apache.hadoop.mapreduce.Reducer.Context context,
			Path innerWorks, String jobName, String writerType) throws Exception{
		
		if (writerType == "SequenceFile"){
			return new NestedWriterSF(context, innerWorks, jobName);
		}
		
		if (writerType == "BufferFile"){
			return new NestedWriterBF(context, innerWorks, jobName);
		}
		
		return null;
	}

	// ============================================================================================

	
}
