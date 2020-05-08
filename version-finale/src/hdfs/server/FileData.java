package hdfs.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import hdfs.FileDescriptionI;

public class FileData implements FileDataI {
	
	private static final long serialVersionUID = 1L;
	private List<Integer> daemons;
	private int numberFragments;
	private FileDescriptionI file;
	
	public FileData(FileDescriptionI file) {
		this.daemons = new ArrayList<>();
		this.numberFragments = 0;
		this.file = file;
	}
	
	public FileDescriptionI getFile() {
		return this.file;
	}
	
	public int getNumberDaemons() {
		return this.daemons.size();
	}
	
	public void addDaemon(int daemon) {
		this.daemons.add(daemon);
	}
	
	public int getNumberFragments() {
		return this.numberFragments;
	}
	
	public void setNumberFragments(int numberFragments) {
		this.numberFragments = numberFragments;
	}
	
	public Iterator<Integer> iterator() {
		return this.daemons.iterator();
	}
	
	public void clear() {
		this.daemons.clear();
		this.numberFragments = 0;
	}

}