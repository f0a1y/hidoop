package hdfs.fileProcessing;

public class FileBreakerSelector implements FileBreakerSelectorI {

	private int fragmentLength;
	
	public FileBreakerSelector(int fragmentLength) {
		this.fragmentLength = fragmentLength;
	}
	
	@Override
	public boolean knowsFileFormat(String fileName) {
		return fileName.endsWith(".txt");
	}
	
	@Override
	public FileBreakerA selectBreaker(String fileName) {
		if (fileName.endsWith(".txt"))
			return new FileBreakerTXT(this.fragmentLength);
		return null;
	}

}
