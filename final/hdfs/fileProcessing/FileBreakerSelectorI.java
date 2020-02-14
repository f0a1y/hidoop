package hdfs.fileProcessing;

public interface FileBreakerSelectorI {
	
	boolean knowsFileFormat(String fileName);
	
	FileBreaker selectBreaker(String fileName);
	
}
