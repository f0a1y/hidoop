package hdfs.daemon;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import hdfs.FileDescriptionI;

public class FragmentData implements FragmentDataI {

	private static final long serialVersionUID = 1L;
	private Map<Integer, String> fragmentFiles;
	private FileDescriptionI file;
	private String path;
	
	public FragmentData(FileDescriptionI file, String path) {
		this.fragmentFiles = new HashMap<>();
		this.file = file;
		this.path = path;
	}
	
	public FileDescriptionI getFile() {
		return this.file;
	}
	
	public String getFragmentsPath() {
		return this.path;
	}
	
	public String getFragmentName(int fragment) {
		return this.fragmentFiles.get(fragment);
	}
	
	public int getNumberFragments() {
		return this.fragmentFiles.size();
	}
	
	public Collection<Integer> getFragments() {
		return this.fragmentFiles.keySet();
	}
	
	public boolean hasFragment(int fragment) {
		return this.fragmentFiles.containsKey(fragment);
	}
	
	public Iterator<Integer> iterator() {
		return this.fragmentFiles.keySet().iterator();
	}
	
	public void addFragment(int fragment, String fragmentName) {
		this.fragmentFiles.put(fragment, fragmentName);
	}
	
	public void removeFragment(int fragment) {
		this.fragmentFiles.remove(fragment);
	}
	
	public void clear() {
		this.fragmentFiles.clear();
	}
	
}
