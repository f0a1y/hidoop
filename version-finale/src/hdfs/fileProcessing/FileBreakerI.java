package hdfs.fileProcessing;

import java.util.List;

public interface FileBreakerI {

	boolean fragment(byte[] data, int limit, int firstFragmentLength, List<byte[]> fragments);
	
}
