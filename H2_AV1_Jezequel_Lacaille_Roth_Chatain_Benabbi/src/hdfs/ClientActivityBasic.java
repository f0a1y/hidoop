package hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import config.ClientConfig;
import config.GeneralConfig;
import hdfs.fileProcessing.FileBreaker;
import hdfs.fileProcessing.FileBreakerSelectorI;
import hdfs.server.Cluster;

public class ClientActivityBasic implements ActivityI {
	
	protected CommunicationStream serverStream;
	protected FileBreakerSelectorI selector;
	
	public ClientActivityBasic(FileBreakerSelectorI selector) {
		this.selector = selector;
	}
	
	@Override
	public boolean start(int command, String fileName) throws IOException {
    	// Connexion avec le server
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

		return result;
	}
    
    private void hdfsWrite(String fileName) throws IOException {
    	// Réception des informations du cluster
    	Cluster cluster = Cluster.receiveClusterData(this.serverStream);
    	cluster.connect();
    	
    	// Envoi de la commande aux noeuds du cluster
    	cluster.sendAllData(1);
    	
    	// Envoi du nom du fichier aux noeuds du cluster
    	byte[] buffer = fileName.getBytes();
    	cluster.sendAllData(buffer, buffer.length);
    	
    	FileInputStream reader = new FileInputStream(fileName);
		FileBreaker breaker = this.selector.selectBreaker(fileName);
    	byte[] frame = null;
		buffer = new byte[breaker.getFragmentLength()];
    	ByteBuffer stock = ByteBuffer.allocate(breaker.getFragmentLength() * 3);
    	List<byte[]> fragments = new ArrayList<>();
		int numberRead;
		int ordre = 0;
    	int limit = 0;
    	int index = 0;
    	
    	// Lecture du fichier
		while ((numberRead = reader.read(buffer)) > 0) {
			
    		// Fragmentation du contenu du fichier
    		stock.put(buffer, 0, numberRead);
    		frame = stock.array();
    		limit = stock.position();
    		index = breaker.fragment(frame, limit, fragments);
    		stock.clear();    		
    		stock.put(frame, index, limit - index);
    		
    		// Envoi des fragments aux noeuds
    		if (fragments.size() > 0) {
    			this.sendFragments(cluster, fragments, ordre);
    	  		ordre += fragments.size();
    	  		fragments.clear();
    		}
		} 
		reader.close();
		
		// Envoi du dernier fragment aux noeuds
    	if (stock.position() > 0) {
    		fragments.add(Arrays.copyOfRange(frame, index, limit));
    	  	this.sendFragments(cluster, fragments, ordre);
    	}
    	cluster.close();
    }
    
    private void sendFragments(Cluster cluster, List<byte[]> fragments, int ordre) throws IOException  {
		// Envoi du nombre de fragments prêts au serveur
    	this.serverStream.sendData(fragments.size());
		
		// Réception des listes des identifiants des noeuds auxquels envoyer les fragmentss
		for (byte[] fragment : fragments) {
			for (int i = 0; i < cluster.getRedundancy(); i++) {
				int node = this.serverStream.receiveDataInt();
				cluster.sendData(node, ordre);
				cluster.sendData(node, fragment, fragment.length);
			}
			ordre++;
		}
    }
    
    private void hdfsRead(String fileName) throws IOException {
    	int fragmentsNumber = this.serverStream.receiveDataInt();
    	if (fragmentsNumber > 0) {

    		// Réception des informations du cluster
    		Cluster cluster = Cluster.receiveClusterData(this.serverStream);
    		cluster.connect();

    		// Envoi de la commande aux noeuds du cluster
    		cluster.sendAllData(2);

    		// Envoi du nom du fichier aux noeuds du cluster
    		byte[] buffer = fileName.getBytes();
    		cluster.sendAllData(buffer, buffer.length);

    		List<Integer> fragments = new ArrayList<>();
    		int[] orders = new int[fragmentsNumber];
    		for (int i = 0; i < fragmentsNumber; i++) {
    			fragments.add(i);
    			orders[i] = -1;
    		}

    		// Réception de la liste des noeuds possédants des fragments du fichier
    		int nodesNumber = this.serverStream.receiveDataInt();
    		List<Integer> nodes = new ArrayList<>();
    		for (int i = 0; i < nodesNumber; i++) {
    			nodes.add(this.serverStream.receiveDataInt());
    		}

    		// Préparation de l'ordre de récupération des fragments du fichier
    		for (Integer node : nodes) {
    			int numberFragmentsNode = cluster.receiveDataInt(node);
    			List<Integer> fragmentsNode = new ArrayList<>();
    			for (int i = 0; i < numberFragmentsNode; i++) {
    				Integer fragment = cluster.receiveDataInt(node);
    				if (fragments.remove(fragment)) {
    					orders[fragment] = node;
    					fragmentsNode.add(fragment);
    				}
    			}
    			cluster.sendData(node, fragmentsNode.size());
    			for (Integer fragment : fragmentsNode) {
    				cluster.sendData(node, fragment);
    			}
    		}

    		// Réception des fragments du fichier
    		FileOutputStream writer = new FileOutputStream(fileName);
    		for (int i = 0; i < fragmentsNumber; i++) {
    			if (orders[i] != -1) {
    				cluster.receiveData(orders[i]);
    				writer.write(cluster.receiveData(orders[i]));
    			}
    		}
    		writer.close();
    	}
    }

}
