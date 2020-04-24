package hdfs.daemon;

import java.io.Serializable;
import java.util.Collection;

import hdfs.FileDescriptionI;

public interface FragmentDataI extends Serializable, Iterable<Integer> {
	
	FileDescriptionI getFile();
	
	String getFragmentsPath();
	
	String getFragmentName(int fragment);
	
	int getNumberFragments();
	
	Collection<Integer> getFragments();
	
	boolean hasFragment(int fragment);
	
	void addFragment(int fragment, String fragmentName);
	
	void removeFragment(int fragment);
	
	void clear();

}
