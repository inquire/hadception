
// TODO Add Factory Description

public class WriterFactory<KEYIN, VALUEIN> {
	
	// TODO add makeWriter details (when running it from a Map task)
	
	@SuppressWarnings("rawtypes")
	public CommonWriterUtils makeWriter(org.apache.hadoop.mapreduce.Mapper.Context context,
			String writerType) throws Exception{

		if (writerType == "SequenceFile"){
			return new NestedWriterSF<KEYIN, VALUEIN>(context);
		}
		
		if (writerType == "BufferFile"){
			return new NestedWriterBF<KEYIN, VALUEIN>(context);
		}
		
		return null;
	}
	
	// TODO add makeWriter details (when running it from a Reduce task)
	
	@SuppressWarnings("rawtypes")
	public CommonWriterUtils makeWriter(org.apache.hadoop.mapreduce.Reducer.Context context,
			String writerType) throws Exception{
		
		if (writerType == "SequenceFile"){
			return new NestedWriterSF<KEYIN, VALUEIN>(context);
		}
		
		if (writerType == "BufferFile"){
			return new NestedWriterBF<KEYIN, VALUEIN>(context);
		}
		
		return null;
	}

}
