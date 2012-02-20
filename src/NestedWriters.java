import org.apache.hadoop.fs.Path;


public class NestedWriters<KEYIN, VALUEIN> {

	NestedWriterSF sequenceWriter;
	NestedWriterBF bufferWriter;
	
	@SuppressWarnings({ "rawtypes" })
	public CommonWriterUtils getWriter(Object writerType, 
			org.apache.hadoop.mapreduce.Mapper.Context context, Path innerWorks, String jobName) 
					throws Exception{
		
		if (writerType.getClass().isInstance(sequenceWriter)){
			return new NestedWriterSF(context, innerWorks, jobName);
		}

			return new NestedWriterBF(context, innerWorks, jobName);
		
	}
	
	@SuppressWarnings({"rawtypes"})
	public CommonWriterUtils getWriter(Object writerType, 
			org.apache.hadoop.mapreduce.Reducer.Context context, Path innerWorks, String jobName) 
					throws Exception{
		if (writerType.getClass().isInstance(bufferWriter)){
			return new NestedWriterBF(context, innerWorks, jobName);
		}
			return new NestedWriterSF(context, innerWorks, jobName);

	}
}
