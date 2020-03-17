package hdfs.daemon;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import config.ClusterConfig;
import hdfs.ActivityI;
import hdfs.CommunicationStream;
import formats.FormatSelectorI;

public class DaemonHDFS extends Thread {

    static HashMap<String, List<FragmentFile>> register;
    static FormatSelectorI selector;
    private int id;
    private CommunicationStream emitterStream;
    
    public static class FragmentFile implements Serializable {
    	
    	private int number;
    	private String fileName;
    	
    	public FragmentFile(int number, String fileName) {
    		this.number = number;
    		this.fileName = fileName;
    	}
    	
    	public int getNumber() {
    		return this.number;
    	}
    	
    	public String getFileName() {
    		return this.fileName;
    	}
    	
    }

    public DaemonHDFS(Socket emitter, int id) {
        super();
        this.emitterStream = new CommunicationStream(emitter);
        this.id = id;
    }
    
    public void run() {
        try {
            
            // Réception de la commande
            int command = this.emitterStream.receiveDataInt();
            
            // Réception du nom du file
            String fileName = new String(this.emitterStream.receiveData()) + "_" + this.id;

            ActivityI activity = ClusterConfig.getDaemonActivity(this.emitterStream, this.id);
            activity.start(command, fileName);
            
            // Déconnexion
            this.emitterStream.close();
        } catch (Exception e) {e.printStackTrace();}
    }

    public static void main(String[] args) {
        try {
        	if (args.length == 1) {
	            int id = Integer.parseInt(args[0]);
	            
				// Création de la collection contenant les numéros des fragments par fichier
				File file = new File("register_" + id + ".ser"); 
				if (file.exists()) {
					try {
						ObjectInputStream objectIS = new ObjectInputStream(new FileInputStream(file));
						register = (HashMap<String, List<FragmentFile>>)objectIS.readObject();
						objectIS.close();
					} catch(IOException e) {
						e.printStackTrace();
						return;
					}
				} else {
					register = new HashMap<>();
				}

				// Objet permettant de récupérer le format adapté au fichier
				selector = ClusterConfig.selector;
	            
	            ServerSocket client = new ServerSocket(ClusterConfig.numPortHDFS[id]);
	            while (true) {
	                DaemonHDFS daemon = new DaemonHDFS(client.accept(), id);
	                daemon.start();
	            }
        	} else {
        		System.out.println("Usage : java hdfs.daemon.DaemonHDFS <id>");
        	}
        } catch (Exception e) {e.printStackTrace();}
    }

}
