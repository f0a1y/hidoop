package hdfs.server;

import config.GeneralConfig;
import formats.FormatWriter;
import formats.KV;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import config.ClusterConfig;
import hdfs.ActivityI;
import hdfs.CommunicationStream;

public class ServerHDFS extends Thread {

    static HashMap<String, FileData> register;
    private CommunicationStream clientStream;
    
    public static class FileData implements Serializable {
    	
    	private int numberFragments;
    	private Set<Integer> nodesID;
    	
    	public FileData() {
    		this.nodesID = new HashSet<>();
    	}
    	
    	public int getNumberFragments() {
    		return this.numberFragments;
    	}
    	
    	public Set<Integer> getNodesID() {
    		return this.nodesID;
    	}
    	
    	public void addNode(List<Integer> nodes) {
    		this.nodesID.addAll(nodes);
    		this.numberFragments++;
    	}
    	
    	public void clear() {
    		this.nodesID.clear();
    		this.numberFragments = 0;
    	}
    
    }

    public ServerHDFS(Socket client) {
        super();
        this.clientStream = new CommunicationStream(client);
    }

    public void run() {
        try {
            
            // Réception de la commande
            int command = this.clientStream.receiveDataInt(); 
        	
            // Réception du nom du fichier
            String fileName = new String(this.clientStream.receiveData());
            
            ActivityI activity = ClusterConfig.getServerActivity(this.clientStream);
            activity.start(command, fileName);
	            
	        // Déconnexion avec le client
	        this.clientStream.close();
	        
        } catch (Exception e) {e.printStackTrace();}
    }
    
    public static void recupererResultats(String fileName, FormatWriter writer) {
    	try {

        	// Connexion avec les noeuds du cluster
        	Cluster cluster = new Cluster(ClusterConfig.nbMachine, 
        								  ClusterConfig.nomMachine, 
        								  ClusterConfig.numPortHDFS, 
        								  ClusterConfig.redundancy);
        	cluster.connect();

	    	// Envoie de la command 2 (hdfsRead) aux noeuds du cluster
        	cluster.sendAllData(2);

    		// Envoi du nom du fichier aux noeuds du cluster
    		byte[] buffer = fileName.getBytes();
    		cluster.sendAllData(buffer, buffer.length);
    		
    		FileData data = register.get(fileName);
    		List<Integer> fragments = new ArrayList<>();
    		int[] orders = new int[data.getNumberFragments()];
    		for (int i = 0; i < data.getNumberFragments(); i++) {
    			fragments.add(i);
    			orders[i] = -1;
    		}

    		// Préparation de l'ordre de récupération des fragments du fichier
    		for (Integer node : data.getNodesID()) {
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
    		for (int i = 0; i < data.getNumberFragments(); i++) {
    			if (orders[i] != -1) {
    				writer.write(new KV(new String(cluster.receiveData(orders[i])), 
							  			new String(cluster.receiveData(orders[i]))));
    			}
    		}
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }    

    public static void main(String[] args) {
    	try {

    		// Création de la collection contenant les ordres d'envoi des fragments dans les noeuds par fichier
    		File fichier = new File("register.ser"); 
    		if (fichier.exists()) {
    			try {
    				ObjectInputStream objectIS = new ObjectInputStream(new FileInputStream("register.ser"));
    				register = (HashMap<String, FileData>)objectIS.readObject();
    				objectIS.close();
    			} catch(IOException e) {
    				e.printStackTrace();
    				return;
    			}
    		} else {
    			register = new HashMap<>();
    		}

    		// Attente d'un client
    		ServerSocket serveurPrimaire = new ServerSocket(GeneralConfig.port);
    		while (true) {
    			ServerHDFS serveur = new ServerHDFS(serveurPrimaire.accept());
    			serveur.start();
    		}
    		
    	} catch (Exception e) {e.printStackTrace();}
    }

} 