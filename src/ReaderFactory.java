

import org.apache.hadoop.fs.Path;

public class ReaderFactory {

	// TODO add makeReader details (when running it from a Map task)
	
	//======================== Factories aggregate reader files in reducers =======================
	
	
	@SuppressWarnings("rawtypes")
	public CommonReaderUtils makeReader(org.apache.hadoop.mapreduce.Mapper.Context context, 
			String agreggatePath, String readerType, String condition) throws Exception{
		
		if(readerType == "SequenceFile"){
			return new NestedReaderSF(context, agreggatePath, condition);
		}
		
		return null;
		
	}
	
	@SuppressWarnings("rawtypes")
	public CommonReaderUtils makeReader(org.apache.hadoop.mapreduce.Reducer.Context context, 
			String agreggatePath, String readerType, String condition) throws Exception{
		
		if(readerType == "SequenceFile"){
			return new NestedReaderSF(context, agreggatePath, condition);
		}
		
		return null;
		
	}

	
	
	//========================== Factories for development with fixed path ========================
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public CommonReaderUtils makeReader(org.apache.hadoop.mapreduce.Mapper.Context context,
			String readerType) throws Exception{

		if (readerType == "SequenceFile"){
			return new NestedReaderSF(context);
		}
		
		if (readerType == "BufferFile"){
			return new NestedReaderBF(context);
		}
		
		return null;
	}
	
	// TODO add makeReader details (when running it from a Reduce task)
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public CommonReaderUtils makeReader(org.apache.hadoop.mapreduce.Reducer.Context context,
			String readerType) throws Exception{
		
		if (readerType == "SequenceFile"){
			return new NestedReaderSF(context);
		}
		
		if (readerType == "BufferFile"){
			return new NestedReaderBF(context);
		}
		
		return null;
	}
	
	// ========================= Factories with custom writer path ================================
	
	@SuppressWarnings("rawtypes")
	public CommonReaderUtils makeReader (org.apache.hadoop.mapreduce.Mapper.Context context, 
			Path innerWorks, String jobName, String readerType, String condition) throws Exception{
		
		if (readerType == "SequenceFile"){
			return new NestedReaderSF(context ,innerWorks, jobName, condition);
		}
		
		/*
		if (readerType == "BufferFile"){
			return new NestedReaderBF(context, innerWorks, jobName);
		}
		*/
		
		return null;
	}
	
	@SuppressWarnings("rawtypes")
	public CommonReaderUtils makeReader (org.apache.hadoop.mapreduce.Reducer.Context context, 
			Path innerWorks, String jobName, String readerType, String condition) throws Exception{
		
		if (readerType == "SequenceFile"){
			return new NestedReaderSF(context ,innerWorks, jobName, condition);
		}
		
		
		/*
		if (readerType == "BufferFile"){
			return new NestedReaderBF(context, innerWorks, jobName);
		}
		*/
		
		return null;
	}
	
	
	
	
	
	
}
