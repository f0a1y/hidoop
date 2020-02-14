package config;

import hdfs.ClientActivityBasic;
import hdfs.CommunicationStream;
import hdfs.fileProcessing.FileBreakerSelectorBasic;
import hdfs.fileProcessing.FileBreakerSelectorI;
import hdfs.ActivityI;

public class ClientConfig {

	// Sélecteur de fragmenteur de fichier
    public static final FileBreakerSelectorI selector = new FileBreakerSelectorBasic(1024);
    
    // Choix du comportement des clients HDFS
    public static ActivityI getClientActivity(CommunicationStream serverStream) {
    	return new ClientActivityBasic(serverStream, selector);
    }
    
}
