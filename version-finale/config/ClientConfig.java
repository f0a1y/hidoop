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
	
	// Taille des fragment en octets
	public static final int fragmentLength = 10000;
    
	// Taille du buffer de lecture
	public static final int readLength = 1000;

	// Sélecteur de fragmenteur de fichier
	public static final FileBreakerSelectorI selector = new FileBreakerSelector(fragmentLength);
    
    // Choix du comportement des clients HDFS
    public static ActivityI getClientActivity(List<List<Command>> commands, List<List<FileDescriptionI>> files) {
    	return new ClientActivity(selector, commands, files);
    }
    
    // Choix du nom sous lequel écrire le fichier
    public static String fileToFileName(FileDescriptionI file) {
		if (file.hasDestinationName())
			return file.getDestinationName();
		else
			return file.getPath() + File.separator + file.getName();
    }
    
}
