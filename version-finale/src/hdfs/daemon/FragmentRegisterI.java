package hdfs.daemon;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import config.ClusterConfig;
import hdfs.FileDescriptionI;

public interface FragmentRegisterI extends Serializable, Iterable<FileDescriptionI> {
    
	int getNumberFiles();
	
    FragmentDataI getData(FileDescriptionI file);
    
    boolean hasData(FileDescriptionI file);
    
    boolean updateData(FileDescriptionI file, int id);
    
	FragmentDataI addData(FileDescriptionI file, int id);
    
    boolean deleteData(FileDescriptionI file);
    
    String createRepertory(FileDescriptionI file);
    
    String createFragment(FileDescriptionI file, int fragment);
    
    static void save(FragmentRegisterI register, int id) {
    	try {
            ObjectOutputStream objectOS = new ObjectOutputStream(new FileOutputStream(ClusterConfig.getDataPath() + "daemon-" + id + "-register.ser"));
            objectOS.writeObject(register);
            objectOS.close();
        } catch(IOException e) {e.printStackTrace();}
    }
	
    static FragmentRegisterI open(int id) {
    	FragmentRegisterI register = null;
		File fichier = new File(ClusterConfig.getDataPath() + "daemon-" + id + "-register.ser"); 
		if (fichier.exists()) {
			try {
				ObjectInputStream objectIS = new ObjectInputStream(new FileInputStream(ClusterConfig.getDataPath() + "daemon-" + id + "-register.ser"));
				register = (FragmentRegisterI)objectIS.readObject();
				objectIS.close();
			} catch(IOException e) {e.printStackTrace();}
			catch(ClassNotFoundException e) {e.printStackTrace();}
		}
		return register;
    }
    
}
