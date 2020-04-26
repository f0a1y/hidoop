package hdfs.daemon;

import java.util.HashMap;
import java.util.Iterator;

import config.ClusterConfig;
import hdfs.FileDescriptionI;

public class FragmentRegister implements FragmentRegisterI {

	private static final long serialVersionUID = 1L;
	private HashMap<FileDescriptionI, FragmentDataI> register;
    
    public FragmentRegister() {
    	this.register = new HashMap<>();
    }
    
    public int getNumberFiles() {
    	return this.register.size();
    }
    
    public FragmentDataI getData(FileDescriptionI file) {
    	return this.register.get(file);
    }
    
    public boolean hasData(FileDescriptionI file) {
    	return this.register.containsKey(file);
    }
    
    public Iterator<FileDescriptionI> iterator() {
    	return this.register.keySet().iterator();
    }
    
    public FragmentDataI addData(FileDescriptionI file, int id) {
    	FragmentDataI data;
    	if (!this.register.containsKey(file)) {
    		data = ClusterConfig.getFragmentData(file, ClusterConfig.fileToRepertory(file, id));
    		this.register.put(file, data);
    	} else {
    		data = this.register.get(file);
    		data.clear();
    	}
    	return data;
    }
    
    public FragmentDataI removeData(FileDescriptionI file) {
		return this.register.remove(file);
    }
    
}
