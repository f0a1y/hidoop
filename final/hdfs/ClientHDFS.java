package hdfs;

import config.ClientConfig;
import config.GeneralConfig;
import java.net.Socket;

public class ClientHDFS {

    public static void main(String[] args) {
    	if (args.length == 2) {
    		String fileName = args[1];
            if (ClientConfig.selector.knowsFileFormat(fileName)) {
		        try {
		        	
		        	// Connexion avec le server
		            Socket server = new Socket(GeneralConfig.host, GeneralConfig.port);
		            CommunicationStream serverStream = new CommunicationStream(server);
		            
		            // Envoie de la commande
		            int command = Integer.parseInt(args[0]);
		            serverStream.sendData(command);
		    		
		    		// Envoie du nom du fichier
		    		byte[] buffer = fileName.getBytes();
		            serverStream.sendData(buffer, buffer.length);

		            ActivityI activity = ClientConfig.getClientActivity(serverStream);
		    	   	activity.start(command, fileName);
		    		
		    		// Déconnexion avec le server
		    		serverStream.close();
		        } catch (Exception e) {e.printStackTrace();}
            } else {
            	System.out.println("format du fichier inconnu");
            }
    	} else {
    		System.out.println("Usage : java hdfs.ClientHDFS <command> <fileName>");
    	}
    }
    
}
