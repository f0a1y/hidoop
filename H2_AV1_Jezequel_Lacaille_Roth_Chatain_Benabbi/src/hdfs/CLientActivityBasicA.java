package hdfs;

import java.io.IOException;
import java.net.Socket;

import config.GeneralConfig;

public abstract class CLientActivityBasicA implements ActivityI {

	protected CommunicationStream serverStream;
	
	@Override
	public void start(int[] command, String[] fileName) throws IOException {
		// TODO Auto-generated method stub// Connexion avec le server
        Socket server = new Socket(GeneralConfig.host, GeneralConfig.port);
        this.serverStream = new CommunicationStream(server);
        
        // Envoie de la commande
        serverStream.sendData(command);
		
		// Envoie du nom du fichier
		byte[] buffer = fileName.getBytes();
        serverStream.sendData(buffer, buffer.length);
        
	    boolean result = true;
		if (command == 1) {
	  		hdfsWrite(fileName);
	   	} else if (command == 2) {
	    	hdfsRead(fileName);
	   	} else {
	   		result = false;
	   	}

		// Déconnexion avec le server
		serverStream.close();
		return false;
	}

}
