package hdfs.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import config.ClusterConfig;
import hdfs.FileDescriptionI;

public interface FileRegisterI extends Serializable, Iterable<FileDescriptionI> {
    
	int getNumberFiles();
	
    FileDataI getData(FileDescriptionI file);
    
    boolean hasData(FileDescriptionI file);

	FileDataI addData(FileDescriptionI file);
    
    FileDataI removeData(FileDescriptionI file);
    
    static void save(FileRegisterI register) {
    	try {
            ObjectOutputStream objectOS = new ObjectOutputStream(new FileOutputStream(ClusterConfig.getDataPath() + "server-register.ser"));
            objectOS.writeObject(register);
            objectOS.close();
        } catch(IOException e) {e.printStackTrace();}
    }
	
    static FileRegisterI open() {
    	FileRegisterI register = null;
		File fichier = new File(ClusterConfig.getDataPath() + "server-register.ser"); 
		if (fichier.exists()) {
			try {
				ObjectInputStream objectIS = new ObjectInputStream(new FileInputStream(ClusterConfig.getDataPath() + "server-register.ser"));
				register = (FileRegisterI)objectIS.readObject();
				objectIS.close();
			} catch(IOException e) {e.printStackTrace();}
			catch(ClassNotFoundException e) {e.printStackTrace();}
		}
		return register;
    }
    
}
