package hdfs;

import java.io.IOException;

public interface ActivityI {
	
	public void start() throws IOException;
	
	public boolean execute(Command command, FileDescriptionI file) throws IOException;
	
	public void terminate() throws IOException;

}
