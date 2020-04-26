package hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import config.ClientConfig;
import hdfs.fileProcessing.FileBreakerA;
import hdfs.fileProcessing.FileBreakerSelectorI;
import hdfs.server.Cluster;

public class ClientActivity extends ClientActivityA {
	
	public ClientActivity(FileBreakerSelectorI selector, List<List<Command>> commands, List<List<FileDescriptionI>> files) {
		super(selector, commands, files);
	}
	
	@Override
	public boolean execute(Command command, FileDescriptionI file) throws IOException {
	    boolean result = true;
		if (command == Command.Upload) {
	  		hdfsWrite(file);
	   	} else if (command == Command.Download) {
	    	hdfsRead(file);
	   	} else if (command == Command.Status) {
	   		hdfsStatus();
	   	} else {
	   		result = false;
	   	}
		return result;
	}
    
    private void hdfsWrite(FileDescriptionI file) throws IOException {
    	FileInputStream reader = new FileInputStream(file.getPath() + file.getName());
    	this.serverStream.sendData(reader.getChannel().size());

    	// Réception des informations du cluster
    	Cluster cluster = Cluster.receiveClusterData(this.serverStream);
    	cluster.connectAll();

    	// Réception de l'ordre d'envoi des fragments
    	int numberFragments = this.serverStream.receiveDataInt();
    	List<Integer> remainingDaemons = new ArrayList<>();
    	int[][] daemons = new int[numberFragments][];
    	for (int i = 0; i < numberFragments; i++) {
    		daemons[i] = new int[cluster.getRedundancy()];
    		for (int j = 0; j < cluster.getRedundancy(); j++) {
    			int daemon = this.serverStream.receiveDataInt();
    			daemons[i][j] = daemon;
    			remainingDaemons.add(daemon);
    		}
    	}
    	
    	// Envoi de la commande aux noeuds du cluster
    	cluster.sendAllData(Command.Upload);
    	
    	// Envoi du nom du fichier aux noeuds du cluster
    	cluster.sendAllData(file);
    	
		FileBreakerA breaker = this.selector.selectBreaker(file.getName());
		int[] currentDaemons = null;
		byte[] buffer = new byte[ClientConfig.readLength];
    	int numberRead, firstFragmentLength, order;
    	firstFragmentLength = order = 0;
    	boolean lastFragmentCompleted, previousFragmentCompleted = true;
    	
    	// Lecture du fichier
		while ((numberRead = reader.read(buffer)) > 0) {
			
    		// Fragmentation du contenu du fichier
	    	List<byte[]> fragments = new ArrayList<>();
			lastFragmentCompleted = breaker.fragment(buffer, numberRead, firstFragmentLength, fragments);
    		
    		// Envoi des fragments aux noeuds
			currentDaemons = daemons[order % daemons.length];
	    	Iterator<byte[]> iterator = fragments.iterator();
	    	byte[] fragment = null;
			for (int i = 1; i <= fragments.size(); i++) {
				fragment = iterator.next();
				if (i > 1 || previousFragmentCompleted)  {
					cluster.sendAllData(currentDaemons, (byte)1);
					cluster.sendAllData(currentDaemons, order);
				} else
					cluster.sendAllData(currentDaemons, (byte)0);
				cluster.sendAllData(currentDaemons, fragment, fragment.length);
				if (i < fragments.size() || lastFragmentCompleted) {
					
					// Fermeture des daemons non nécessaires
					for (Integer daemon : currentDaemons) {
						remainingDaemons.remove(daemon);
						if (!remainingDaemons.contains(daemon))
							cluster.close(daemon);
					}
					
					order++;
					firstFragmentLength = 0;
				} else
					firstFragmentLength = fragment.length;
				currentDaemons = daemons[order % daemons.length];
			}
			previousFragmentCompleted = lastFragmentCompleted;
		} 
		if (!previousFragmentCompleted)	{
			cluster.sendAllData(currentDaemons, (byte)1);
			order++;
		}
		
		reader.close();
    	
    	// Envoi du nombre réel de fragments
    	this.serverStream.sendData(order);
    	
    	cluster.closeAll();
    }
    
    private void hdfsRead(FileDescriptionI file) throws IOException {
    	int numberDaemons = this.serverStream.receiveDataInt();
    	if (numberDaemons > 0) {

    		// Réception des informations du cluster
    		Cluster cluster = Cluster.receiveClusterData(this.serverStream);
    		cluster.connectAll();
    		
    		// Envoi de la commande aux noeuds du cluster
    		cluster.sendAllData(Command.Download);
        	
        	// Envoi du nom du fichier aux noeuds du cluster
        	cluster.sendAllData(file);
    		
    		// Réception des fragments des noeuds 
    		Set<Integer> daemons = new HashSet<>();
    		int fragmentMax = 0;
    		for (int i = 0; i < numberDaemons; i++) {
    			int daemon = this.serverStream.receiveDataInt();
    			int numberFragments = this.serverStream.receiveDataInt();
				cluster.sendData(daemon, numberFragments);
    			for (int j = 0; j < numberFragments; j++) {
    				int fragment = this.serverStream.receiveDataInt();
    				if (fragment + 1 > fragmentMax) 
    					fragmentMax = fragment + 1;
    				cluster.sendData(daemon, fragment);
    			}
    			daemons.add(daemon);
    		}

        	// Etablissement de l'ordre de réception des fragments
    		int[] order = new int[fragmentMax];
    		for (int i = 0; i < fragmentMax; i++)
    			order[i] = -1;
    		Set<Integer> daemonsUsed = new HashSet<>();
    		for (Integer daemon : daemons) {
    			int numberFragments = cluster.receiveDataInt(daemon);
    			List<Integer> fragments = new ArrayList<>();
    			for (int i = 0; i < numberFragments; i++) {
    				int fragment = cluster.receiveDataInt(daemon);
    				if (order[fragment] == -1) {
    					order[fragment] = daemon;
    					fragments.add(fragment);
    					daemonsUsed.add(daemon);
    				}
    			}
    			cluster.sendData(daemon, fragments.size());
    			for (int fragment : fragments)
    				cluster.sendData(daemon, fragment);
    		}
    		
    		// Fermeture des noeuds non nécessaires
    		cluster.closeAllExcept(daemonsUsed);

    		// Réception des fragments du fichier
    		FileOutputStream writer = new FileOutputStream(ClientConfig.fileToFileName(file));
    		File f = new File(ClientConfig.fileToFileName(file));
    		System.out.println("test " +f.getAbsolutePath());
    		for (int i = 0; i < fragmentMax; i++) {
    			if (order[i] != -1) {
    				cluster.sendData(order[i], i);
    				while (cluster.receiveDataByte(order[i]) == 0) {
    					cluster.receiveData(order[i]);
    					writer.write(cluster.receiveData(order[i]));
    				}
    			}
    		}
    		writer.close();
    	}
    }

    private void hdfsStatus() throws IOException {
    	int numberFiles = this.serverStream.receiveDataInt();
    	for (int i = 0; i < numberFiles; i++) {
    		FileDescriptionI file = this.serverStream.receiveFile();
    		System.out.println(file);
    	}
    }
    
}
