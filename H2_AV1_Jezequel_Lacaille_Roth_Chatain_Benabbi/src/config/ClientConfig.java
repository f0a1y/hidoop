package config;

import java.util.List;

import hdfs.ActivityI;
import hdfs.ClientActivity;
import hdfs.Command;
import hdfs.FileDescriptionI;
import hdfs.fileProcessing.FileBreakerSelectorBasic;
import hdfs.fileProcessing.FileBreakerSelectorI;

public class ClientConfig {
	
	public static final int fragmentLength = (int)Math.pow(2, 20);
    
	public static final int readLength = 1024;

	// Sélecteur de fragmenteur de fichier
	public static final FileBreakerSelectorI selector = new FileBreakerSelectorBasic(fragmentLength);
    
    // Choix du comportement des clients HDFS
    public static ActivityI getClientActivity(List<List<Command>> commands, List<List<FileDescriptionI>> files) {
    	return new ClientActivity(selector, commands, files);
    }
    
    public static String fileToFileName(FileDescriptionI file) {
		if (file.hasDestinationName())
			return file.getPath() + file.getDestinationName();
		else
			return file.getPath() + file.getName();
    }
    
}
