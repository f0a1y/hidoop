package hdfs.server;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import hdfs.FileDescriptionI;

public class FileData implements FileDataI {
	
	private static final long serialVersionUID = 1L;
	private Map<Integer, Collection<Integer>> daemons;
	private Map<Integer, Collection<Integer>> fragments;
	private FileDescriptionI file;
	
	public FileData(FileDescriptionI file) {
		this.daemons = new HashMap<>();
		this.fragments = new HashMap<>();
		this.file = file;
	}
	
	public FileDescriptionI getFile() {
		return this.file;
	}
	
	public int getNumberDaemons() {
		return this.daemons.size();
	}
	
	public int getNumberFragments() {
		return this.fragments.size();
	}
	
	public Iterator<Integer> iterator() {
		return this.daemons.keySet().iterator();
	}
	
	public Collection<Integer> getDaemons() {
		return this.daemons.keySet();
	}
	
	public Collection<Integer> getFragmentDaemons(int fragment) {
		if (this.fragments.containsKey(fragment)) 
			return this.fragments.get(fragment);
		else
			return null;
	}
	
	public Collection<Integer> getFragments() {
		return this.fragments.keySet();
	}
	
	public Collection<Integer> getDaemonFragments(int daemon) {
		if (this.daemons.containsKey(daemon)) 
			return this.daemons.get(daemon);
		else
			return null;
	}
	
	public void addDaemonFragments(int daemon, Collection<Integer> fragments) {
		for (Integer fragment : fragments) {
			if (this.fragments.containsKey(fragment))
				this.fragments.get(fragment).add(daemon);
			else {
				Set<Integer> daemons = new HashSet<>();
				daemons.add(daemon);
				this.fragments.put(fragment, daemons);
			}
		}
		if (this.daemons.containsKey(daemon))
			this.daemons.get(daemon).addAll(fragments);
		else
			this.daemons.put(daemon, fragments);
	}
	
	public void addDaemonFragment(int daemon, int fragment) {
		if (this.fragments.containsKey(fragment))
			this.fragments.get(fragment).add(daemon);
		else {
			Set<Integer> daemons = new HashSet<>();
			daemons.add(daemon);
			this.fragments.put(fragment, daemons);
		}
		if (this.daemons.containsKey(daemon))
			this.daemons.get(daemon).add(fragment);
		else {
			Set<Integer> fragments = new HashSet<>();
			fragments.add(fragment);
			this.daemons.put(daemon, fragments);
		}
	}
	
	public void removeDaemonFragments(int daemon, Collection<Integer> fragments) {
		for (Integer fragment : fragments) {
			if (this.fragments.containsKey(fragment)) {
				this.fragments.get(fragment).remove(daemon);
				if (this.fragments.get(fragment).isEmpty())
					this.fragments.remove(fragment);
			}
		}
		if (this.daemons.containsKey(daemon))
			this.daemons.get(daemon).removeAll(fragments);
	}
	
	public void removeDaemonFragment(int daemon, int fragment) {
		if (this.fragments.containsKey(fragment)) {
			this.fragments.get(fragment).remove(daemon);
			if (this.fragments.get(fragment).isEmpty())
				this.fragments.remove(fragment);
		}
		if (this.daemons.containsKey(daemon))
			this.daemons.get(daemon).remove(fragment);
	}
	
	public void clear() {
		this.daemons.clear();
		this.fragments.clear();
	}

}