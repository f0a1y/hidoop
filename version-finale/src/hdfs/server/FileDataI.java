package hdfs.server;

import java.io.Serializable;

import hdfs.FileDescriptionI;

public interface FileDataI extends Serializable, Iterable<Integer> {
	
	FileDescriptionI getFile();
	
	int getNumberDaemons();
	
	void addDaemon(int daemon);
	
	int getNumberFragments();
	
	void setNumberFragments(int numberFragments);
	
	void clear();
	
}
