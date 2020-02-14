package hdfs;

import java.io.IOException;

public interface ActivityI {
	
	public boolean start(int command, String fileName) throws IOException;

}
