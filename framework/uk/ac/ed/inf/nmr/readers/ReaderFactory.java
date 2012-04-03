package uk.ac.ed.inf.nmr.readers;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import uk.ac.ed.inf.nmr.mapreduce.NestedMapper;
import uk.ac.ed.inf.nmr.mapreduce.NestedReducer;
import uk.ac.ed.inf.nmr.mapreduce.JobTrigger;

/**
 * A ReaderFactory responsible for reading the output of the nested job back into it's task.
 * @author Daniel Stanoescu
 *
 */

public class ReaderFactory {

	//======================== Factories aggregate reader files in reducers =======================
	
	/**
	 * Makes a reader for the {@link NestedMapper} to read from the output of an intermediary file.
	 * 
	 * @param context Uses the {@link Context} of the Hadoop {@link Mapper}.
	 * @param agreggatePath Requires the path from where to read the intermediate file.
	 * @param readerType The type of reader required to read the intermediate file
	 * @param condition The value of the {@link JobTracker} used to find the path of the intermediate file.
	 * @return Sanity Check.
	 * @throws Exception
	 */
	
	@SuppressWarnings("rawtypes")
	public CommonReaderUtils makeReader(org.apache.hadoop.mapreduce.Mapper.Context context, 
			String agreggatePath, String readerType, String condition) throws Exception{
		
		if(readerType == "SequenceFile"){
			return new NestedReaderSF(context, agreggatePath, condition);
		}
		
		return null;
	}
	
	/**
	 * Makes a reader for the {@link NestedReducer} to read from the output of an intermediary file.
	 * 
	 * @param context Uses the {@link Context} of the Hadoop {@link Reducer}.
	 * @param agreggatePath Requires the path from where to read the intermediate file.
	 * @param readerType The type of reader required to read the intermediate file
	 * @param condition The value of the {@link JobTracker} used to find the path of the intermediate file.
	 * @return Sanity Check.
	 * @throws Exception
	 */
	
	@SuppressWarnings("rawtypes")
	public CommonReaderUtils makeReader(org.apache.hadoop.mapreduce.Reducer.Context context, 
			String agreggatePath, String readerType, String condition) throws Exception{
		
		if(readerType == "SequenceFile"){
			return new NestedReaderSF(context, agreggatePath, condition);
		}
		
		return null;
	}
	
	// ========================= Factories with custom writer path ================================
	
	/**
	 * Makes a reader for the {@link NestedMapper} to read from the output of a nested job.
	 *
	 * @param context Uses the {@link Context} of the Hadoop {@link Mapper}.
	 * @param innerWorks Requires a path where to write the file it creates.
	 * @param jobName Uses a jobName to construct the path to the file.
	 * @param readerType Creates the appropriate writer for the file it requires
	 * @param condition Uses the {@link JobTrigger} condition to find the right reader.
	 * @return Sanity check
	 * @throws Exception
	 */
	
	@SuppressWarnings("rawtypes")
	public CommonReaderUtils makeReader (org.apache.hadoop.mapreduce.Mapper.Context context, 
			Path innerWorks, String jobName, String readerType, String condition) throws Exception{
		
		if (readerType == "SequenceFile"){
			return new NestedReaderSF(context ,innerWorks, jobName, condition);
		}
		
		if (readerType == "BufferFile"){
			return new NestedReaderBF(context, innerWorks, jobName, condition);
		}
		
		return null;
	}
	
	/**
	 * Makes a reader for the {@link NestedReducer} to read from the output of a nested job.
	 *
	 * @param context Uses the {@link Context} of the Hadoop {@link Reducer}.
	 * @param innerWorks Requires a path where to write the file it creates.
	 * @param jobName Uses a jobName to construct the path to the file.
	 * @param readerType Creates the appropriate writer for the file it requires
	 * @param condition Uses the {@link JobTrigger} condition to find the right reader.
	 * @return Sanity check
	 * @throws Exception
	 */
	
	@SuppressWarnings("rawtypes")
	public CommonReaderUtils makeReader (org.apache.hadoop.mapreduce.Reducer.Context context, 
			Path innerWorks, String jobName, String readerType, String condition) throws Exception{
		
		if (readerType == "SequenceFile"){
			return new NestedReaderSF(context ,innerWorks, jobName, condition);
		}
		
		if (readerType == "BufferFile"){
			return new NestedReaderBF(context, innerWorks, jobName, condition);
		}
		
		return null;
	}
}
