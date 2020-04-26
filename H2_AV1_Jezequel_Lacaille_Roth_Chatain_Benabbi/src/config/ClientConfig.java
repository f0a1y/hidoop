package config;

import java.io.File;
import java.util.List;

import hdfs.ActivityI;
import hdfs.ClientActivity;
import hdfs.Command;
import hdfs.FileDescriptionI;
import hdfs.fileProcessing.FileBreakerSelector;
import hdfs.fileProcessing.FileBreakerSelectorI;

public class ClientConfig {
	
	public static final int fragmentLength = 1024;
    
	public static final int readLength = 1024;

	// Sélecteur de fragmenteur de fichier
	public static final FileBreakerSelectorI selector = new FileBreakerSelector(fragmentLength);
    
    // Choix du comportement des clients HDFS
    public static ActivityI getClientActivity(List<List<Command>> commands, List<List<FileDescriptionI>> files) {
    	return new ClientActivity(selector, commands, files);
    }
    
    public static String fileToFileName(FileDescriptionI file) {
		if (file.hasDestinationName())
			return file.getDestinationName();
		else
			return file.getPath() + File.separator + file.getName();
    }
    
}
