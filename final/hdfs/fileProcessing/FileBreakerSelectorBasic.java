package hdfs.fileProcessing;

public class FileBreakerSelectorBasic implements FileBreakerSelectorI {

	private int fragmentLength;
	
	public FileBreakerSelectorBasic(int fragmentLength) {
		this.fragmentLength = fragmentLength;
	}
	
	@Override
	public boolean knowsFileFormat(String fileName) {
		return fileName.endsWith(".txt");
	}
	
	@Override
	public FileBreaker selectBreaker(String fileName) {
		if (fileName.endsWith(".txt"))
			return new FileBreakerTXT(this.fragmentLength);
		return null;
	}

}
