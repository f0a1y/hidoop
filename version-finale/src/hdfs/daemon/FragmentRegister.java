package hdfs.daemon;

import java.io.File;
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
    
    public boolean updateData(FileDescriptionI file, int id) {
    	if (this.register.containsKey(file)) {
			FragmentDataI data = this.register.get(file);
			data.getFile().update(file);
			String path = ClusterConfig.fileToRepertory(data.getFile(), id);
		    File repertory = new File(data.getFragmentsPath());
			data.setFragmentsPath(path);
		    if (repertory.exists())
		    	repertory.renameTo(new File(path));
		    return true;
    	}
    	return false;
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
    
    public boolean deleteData(FileDescriptionI file) {
    	if (this.register.containsKey(file)) {
			FragmentDataI data = this.register.remove(file);
		    String repertoryName = data.getFragmentsPath();
	    	File repertory = new File(repertoryName);
		    if (repertory.exists()) 
				for (File fragmentFile : repertory.listFiles()) 
					fragmentFile.delete();
	    	repertory.delete();
	    	return true;
    	}
		return false;
    }
    
    public String createRepertory(FileDescriptionI file) {
    	String repertoryName = null;
    	if (this.register.containsKey(file)) {
			FragmentDataI data = this.register.get(file);
		    repertoryName = data.getFragmentsPath();
		    File repertory = new File(repertoryName);
		    repertory.mkdir();
    	}
    	return repertoryName;
    }
    
    public String createFragment(FileDescriptionI file, int fragment) {
    	String fragmentName = null;
    	if (this.register.containsKey(file)) {
			FragmentDataI data = this.register.get(file);
	        fragmentName = ClusterConfig.fragmentToName(fragment);
	        data.addFragment(fragment, fragmentName);
    	}
    	return fragmentName;
    }
    
}
