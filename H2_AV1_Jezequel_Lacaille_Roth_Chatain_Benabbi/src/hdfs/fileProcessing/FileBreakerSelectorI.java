package hdfs.fileProcessing;

public interface FileBreakerSelectorI {
	
	boolean knowsFileFormat(String fileName);
	
	FileBreakerA selectBreaker(String fileName);
	
}
