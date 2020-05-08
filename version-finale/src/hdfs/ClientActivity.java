package hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import config.ClientConfig;
import hdfs.daemon.FragmentDataI;
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
    	int[][] daemons = new int[numberFragments][];
    	for (int i = 0; i < numberFragments; i++) {
    		daemons[i] = new int[cluster.getRedundancy()];
    		for (int j = 0; j < cluster.getRedundancy(); j++) {
    			int daemon = this.serverStream.receiveDataInt();
    			daemons[i][j] = daemon;
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
					order++;
					firstFragmentLength = 0;
				} else
					firstFragmentLength += fragment.length;
				currentDaemons = daemons[order % daemons.length];
			}
			previousFragmentCompleted = lastFragmentCompleted;
		} 
		if (!previousFragmentCompleted)	{
			cluster.sendAllData(currentDaemons, (byte)1);
			order++;
		}
    	cluster.closeAll();
		
		reader.close();
    	
    	// Envoi du nombre réel de fragments
    	this.serverStream.sendData(order);
    	
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
        	
        	int numberFragments = this.serverStream.receiveDataInt();
    		int[] order = new int[numberFragments];
			HashMap<Integer, List<Integer>> fragments = new HashMap<>();
			for (int i = 0; i < numberFragments; i++) {
				fragments.put(i, new ArrayList<>());
				order[i] = -1;
			}
			
			for (int i = 0; i < numberDaemons; i++) {
    			FragmentDataI fragmentData = cluster.receiveFragmentData(i);
    			if (fragmentData!= null) 
    				for (Integer fragment : fragmentData)
    					fragments.get(fragment).add(i);
			}
			for (int i = 0; i < numberFragments; i++) {
				List<Integer> daemons = fragments.get(i);
				if (daemons.size() > 0)
					order[i] = daemons.remove(i % daemons.size());
			}

    		// Réception des fragments du fichier
    		FileOutputStream writer = new FileOutputStream(ClientConfig.fileToFileName(file));
    		int fragment = 0;
    		while (fragment < numberFragments) {
				boolean error = true;
    			if (order[fragment] != -1) {
    				cluster.sendData(order[fragment], (byte)0);
    				cluster.sendData(order[fragment], fragment);
    				while (cluster.receiveDataByte(order[fragment]) == 0) {
    					cluster.receiveData(order[fragment]);
    					writer.write(cluster.receiveData(order[fragment]));
    					error = false;
    				}
    			}
				if (error && fragments.get(fragment).size() > 0) {
					List<Integer> daemons = fragments.get(fragment);
					order[fragment] = daemons.remove(fragment % daemons.size());
				} else
					fragment++;
    		}
    		cluster.sendAllData((byte)1);
    		cluster.closeAll();
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
