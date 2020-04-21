package hdfs.fileProcessing;

public abstract class FileBreakerA implements FileBreakerI {
	
	private int fragmentLength;
	
	public FileBreakerA(int fragmentLength) {
		this.fragmentLength = fragmentLength;
	}
	
	public int getFragmentLength() {
		return this.fragmentLength;
	}

}
