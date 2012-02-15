
public class NestedWriters<KEYIN, VALUEIN> {

	NestedWriterSF<KEYIN, VALUEIN> sequenceWriter;
	NestedWriterBF<KEYIN, VALUEIN> bufferWriter;
	
	@SuppressWarnings({ "rawtypes" })
	public CommonWriterUtils getWriter(Object writerType, 
			org.apache.hadoop.mapreduce.Mapper.Context context) throws Exception{
		if (writerType.getClass().isInstance(sequenceWriter)){
			return new NestedWriterSF(context);
		}

			return new NestedWriterBF(context);
		
	}
	
	@SuppressWarnings({"rawtypes"})
	public CommonWriterUtils getWriter(Object writerType, 
			org.apache.hadoop.mapreduce.Reducer.Context context) throws Exception{
		if (writerType.getClass().isInstance(bufferWriter)){
			return new NestedWriterBF(context);
		}
			return new NestedWriterSF(context);

	}
}
