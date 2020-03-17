package hdfs.fileProcessing;

public abstract class FileBreaker implements FileBreakerI {
	
	private int fragmentLength;
	
	public FileBreaker(int fragmentLength) {
		this.fragmentLength = fragmentLength;
	}
	
	public int getFragmentLength() {
		return this.fragmentLength;
	}

}
