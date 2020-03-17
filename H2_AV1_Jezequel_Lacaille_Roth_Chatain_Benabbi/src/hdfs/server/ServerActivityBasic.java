package hdfs.server;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import config.ClusterConfig;
import config.GeneralConfig;
import formats.FormatWriter;
import formats.KV;
import hdfs.ActivityI;
import hdfs.CommunicationStream;
import hdfs.server.ServerHDFS.FileData;

public class ServerActivityBasic implements ActivityI {

    static Random rand = new Random();
	protected CommunicationStream clientStream;
	protected HashMap<String, FileData> register = ServerHDFS.register;
	
	public ServerActivityBasic(CommunicationStream clientStream) {
		this.clientStream = clientStream;
	}

	@Override
	public boolean start(int command, String fileName) throws IOException {
	    boolean result = true;
		if (command == 1) {
            this.hdfsWrite(fileName);
        } else if (command == 2) {
            this.hdfsRead(fileName);
        } else if (command == 3) {
			this.hdfsDelete(fileName);
	   	} else {
	   		result = false;
	   	}
		return result;
	}
	
    private void sendClusterData() throws IOException{
    	Cluster cluster = new Cluster(ClusterConfig.nbMachine, 
    								  ClusterConfig.nomMachine, 
    								  ClusterConfig.numPortHDFS, 
    								  ClusterConfig.redundancy);
    	cluster.sendClusterData(this.clientStream);
    }
    
    private void hdfsWrite(String fileName) throws IOException {
        // Création de la liste contenant l'ordre d'envoi des fragments dans les noeuds
        FileData data = register.get(fileName);
        if (data == null) {
        	data = new FileData();
        	register.put(fileName, data);
        } else {
          	data.clear();
        }
        
    	// Envoi des informations des nodes au client
    	this.sendClusterData();
    	
    	// Préparation de la liste contenant tous les identifiants des noeuds
    	ArrayList<Integer> nodesIDs = new ArrayList<>();
		for (int i = 0; i < ClusterConfig.nbMachine; i++) {
			nodesIDs.add(i);
		}
		
		int fragmentsNumber;
    	while ((fragmentsNumber = this.clientStream.receiveDataInt()) > 0) {
            for (int i = 0; i < fragmentsNumber; i++) {
            	
	    		// Liste contenant les identifiants des noeuds sur lesquels sera envoyé le fragment
	    		List<Integer> nodes = new ArrayList<>(ClusterConfig.redundancy);
	    		
	    		// Envoi des identifiants des nodes sur lesquels envoyer le fragment au client
	    		for (int j = ClusterConfig.redundancy; j > 0; j--) {
	    	        int node = nodesIDs.remove(rand.nextInt(j));
	    	        this.clientStream.sendData(node);
	    	    	nodes.add(node);
	    		}
    	    	data.addNode(nodes);
	    		
	    		nodesIDs.addAll(nodes);
            }
    	}
    	
    	// Enregisterment de la liste contenant l'ordre d'envoi des fragments dans les noeuds
    	try {
            ObjectOutputStream objectOS = new ObjectOutputStream(new FileOutputStream("register.ser"));
            objectOS.writeObject(register);
            objectOS.close();
        } catch(IOException e) {
        	e.printStackTrace();
        }
    }
    
    private void hdfsRead(String fileName) throws IOException {
        // Récupération de la liste contenant l'ordre d'envoi des fragments dans les noeuds
    	if (register.containsKey(fileName)) {
    		FileData data = register.get(fileName);
	    	
	    	// Envoi du nombre de fragments du fichier au client
	    	this.clientStream.sendData(data.getNumberFragments());

	    	// Envoi des informations des nodes au client
	    	this.sendClusterData();
	    	
	    	// Envoi de la liste des identifiants des noeuds possédants des fragments du fichier au client
	    	this.clientStream.sendData(data.getNodesID().size());
	    	for (Integer node : data.getNodesID()) {
	    		this.clientStream.sendData(node);
	    	}
    	}
    }
  
	private void hdfsDelete(String fileName) throws IOException {
	    if (register.containsKey(fileName)) {
	            
			// Suppression des données du fichier du register
			FileData data = register.remove(fileName);
						
			// Connexion avec les noeuds du cluster
			List<CommunicationStream> nodesStreams = new ArrayList<>();
			for (Integer id : data.getNodesID()) {
				Socket node = new Socket(ClusterConfig.nomMachine[id], ClusterConfig.numPortHDFS[id]);
				nodesStreams.add(new CommunicationStream(node));
			}
		
	        byte[] buffer = fileName.getBytes();
			
			// Envoi des informations aux noeuds du cluster
			for (CommunicationStream stream : nodesStreams) {
						
				// Envoi de la commande aux noeuds du cluster
				stream.sendData(3);
			
				// Envoi du nom du fichier aux noeuds du cluster
				stream.sendData(buffer, buffer.length);
        	}
		        	
		    // Déconnexion avec les noeuds du cluster
			for (CommunicationStream stream : nodesStreams) {
				stream.close();
			}
		}
	}

}
