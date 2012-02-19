
public class ReaderFactory {

	// TODO add makeReader details (when running it from a Map task)
	
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
	
	@SuppressWarnings("rawtypes")
	public CommonReaderUtils makeReader(org.apache.hadoop.mapreduce.Reducer.Context context,
			String writerType) throws Exception{
		
		if (writerType == "SequenceFile"){
			return new NestedReaderSF(context);
		}
		
		if (writerType == "BufferFile"){
			return new NestedReaderBF(context);
		}
		
		return null;
	}
}
