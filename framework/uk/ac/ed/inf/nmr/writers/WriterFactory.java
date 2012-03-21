package uk.ac.ed.inf.nmr.writers;
//package uk.ac.ed.inf.nmr.writers;


import org.apache.hadoop.fs.Path;


// TODO Add Factory Description

public class WriterFactory {
	
	// TODO add makeWriter details (when running it from a Map task)
	
	// ==================== Factories for development with fixed path =============================
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public CommonWriterUtils makeWriter(org.apache.hadoop.mapreduce.Mapper.Context context, 
			String writerType) throws Exception{
		
		if(writerType == "SequenceFile"){
			return new NestedWriterSF(context);
		}
		
		if(writerType == "BufferFile"){
			return new NestedWriterBF(context);
		}
		
		return null;
	}
	
	@Deprecated
	@SuppressWarnings("rawtypes")
	public CommonWriterUtils makeWriter(org.apache.hadoop.mapreduce.Reducer.Context context,
			String writerType) throws Exception{
		
		if(writerType == "SequenceFile"){
			return new NestedWriterSF(context);
		}
		
		if(writerType == "BufferFile"){
			return new NestedWriterBF(context);
		}
		
		return null;
	}
	
	// ========================= Factories with custom writer path ================================
	
	@SuppressWarnings("rawtypes")
	public CommonWriterUtils makeWriter(org.apache.hadoop.mapreduce.Mapper.Context context, 
			Path innerWorks, String jobName, String writerType) throws Exception{

		if (writerType == "SequenceFile"){
			System.out.println("i'm here");
			return new NestedWriterSF(context, innerWorks, jobName);
		}
		
		
		if (writerType == "BufferFile"){
			return new NestedWriterBF(context, innerWorks, jobName);
		}
		
		
		return null;
	}
	
	// TODO add makeWriter details (when running it from a Reduce task)
	
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
