package hdfs.server;

import java.io.Serializable;
import java.util.Collection;

import hdfs.FileDescriptionI;

public interface FileDataI extends Serializable, Iterable<Integer> {
	
	FileDescriptionI getFile();
	
	int getNumberDaemons();
	
	int getNumberFragments();
	
	Collection<Integer> getDaemons();
	
	Collection<Integer> getFragmentDaemons(int fragment);
	
	Collection<Integer> getFragments();
	
	Collection<Integer> getDaemonFragments(int daemon);
	
	void addDaemonFragments(int daemon, Collection<Integer> fragments);
	
	void addDaemonFragment(int daemon, int fragment);
	
	void removeDaemonFragments(int daemon, Collection<Integer> fragments);
	
	void removeDaemonFragment(int daemon, int fragment);
	
	void clear();
	
}
