package hdfs.fileProcessing;

import java.util.List;

public interface FileBreakerI {

	int fragment(byte[] data, int length, List<byte[]> fragments);
	
}
