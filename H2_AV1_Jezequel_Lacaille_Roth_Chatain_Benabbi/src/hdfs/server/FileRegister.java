package hdfs.server;

import java.util.HashMap;
import java.util.Iterator;

import config.ClusterConfig;
import hdfs.FileDescriptionI;

public class FileRegister implements FileRegisterI {

	private static final long serialVersionUID = 1L;
	private HashMap<FileDescriptionI, FileDataI> register;
    
    public FileRegister() {
    	this.register = new HashMap<>();
    }
    
    public int getNumberFiles() {
    	return this.register.size();
    }

	public FileDataI getData(FileDescriptionI file) {
    	return this.register.get(file);
	}

	public boolean hasData(FileDescriptionI file) {
    	return this.register.containsKey(file);
	}
    
    public Iterator<FileDescriptionI> iterator() {
    	return this.register.keySet().iterator();
    }

	public FileDataI addData(FileDescriptionI file) {
		FileDataI data;
    	if (!this.register.containsKey(file)) {
    		data = ClusterConfig.getFileData(file);
    		this.register.put(file, data);
    	} else {
    		data = this.register.get(file);
    		data.clear();
    	}
    	return data;
	}

	public FileDataI removeData(FileDescriptionI file) {
		return this.register.remove(file);
	}
	
}
