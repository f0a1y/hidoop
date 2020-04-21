package hdfs.server;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import config.ClientConfig;
import config.ClusterConfig;
import hdfs.Command;
import hdfs.FileDescriptionI;

public class ServerActivity extends ServerActivityA {

    static Random rand = new Random();
	
	public ServerActivity(Socket client) {
		super(client);
	}
    
	@Override
	public boolean execute(Command command, FileDescriptionI file) throws IOException {
	    boolean result = true;
		if (command == Command.Upload) {
            this.hdfsWrite(file);
        } else if (command == Command.Download) {
            this.hdfsRead(file);
        } else if (command == Command.Delete) {
			this.hdfsDelete(file);
	   	} else if (command == Command.Update) {
			this.hdfsUpdate(file);
	   	} else if (command == Command.Status) {
			this.hdfsStatus();
	   	} else {
	   		result = false;
	   	}
		return result;
	}
	
    private void sendClusterData(int numberDaemons, String[] hostNames, int[] ports, int redundancy) throws IOException{
    	Cluster cluster = new Cluster(numberDaemons, hostNames, ports, redundancy);
    	cluster.sendClusterData(this.clientStream);
    }
    
    private void hdfsWrite(FileDescriptionI file) throws IOException {
        // Création de la liste contenant l'ordre d'envoi des fragments dans les noeuds
        FileDataI data = register.addData(file);
        
        // Récéption de la taille du fichier
        long fileLength = this.clientStream.receiveDataLong();
        int numberFragments = (int)Math.ceil((double)fileLength / (double)ClientConfig.fragmentLength);
        
        List<Integer> daemons = new ArrayList<>();
        List<Integer> order = new ArrayList<>();
        List<List<Integer>> daemonsFragments = new ArrayList<>(numberFragments);
    	int[] daemonIds = new int[ClusterConfig.nbMachine];
    	List<Integer> ids = new ArrayList<>(ClusterConfig.nbMachine);
		for (int i = 0; i < ClusterConfig.nbMachine; i++)
			ids.add(i);
		int numberDaemons = 0;

        for (int i = 0; i < numberFragments; i++) {

    		// Liste contenant les identifiants des noeuds sur lesquels sera envoyé le fragment
    		List<Integer> daemonFragments = new ArrayList<>(ClusterConfig.redundancy);
    		for (int j = 0; j < ClusterConfig.redundancy; j++) {
    	        int daemon = ids.remove(rand.nextInt(ids.size()));
    	        if (!daemons.contains(daemon)) {
    	        	daemons.add(daemon);
    	        	daemonIds[daemon] = numberDaemons;
        	        order.add(numberDaemons++);
    	        } else
    	        	order.add(daemonIds[daemon]);
    	        daemonFragments.add(daemon);
    		}
	    	daemonsFragments.add(daemonFragments);
    		ids.addAll(daemonFragments);
        }
        
        String[] hostNames = new String[numberDaemons];
        int[] ports = new int[numberDaemons];
        Iterator<Integer> iterator = daemons.iterator();
        for (int i = 0; i < numberDaemons; i++) {
        	int daemon = iterator.next();
        	hostNames[i] = ClusterConfig.nomMachine[daemon];
        	ports[i] = ClusterConfig.numPortHDFS[daemon];
        }
        
    	// Envoi des informations des nodes au client
    	this.sendClusterData(numberDaemons, hostNames, ports, ClusterConfig.redundancy);

    	// Envoi de l'ordre d'envoi des fragments
        this.clientStream.sendData(numberFragments);
        for (Integer daemon : order)
        	this.clientStream.sendData(daemon);
    	
        int realNumberFragments = this.clientStream.receiveDataInt();
        Iterator<List<Integer>> iteratorFragments = null;
        for (int i = 0; i < realNumberFragments; i++) {
        	if (i % numberFragments == 0)
        		iteratorFragments = daemonsFragments.iterator();
        	for (Integer daemon : iteratorFragments.next())
        		data.addDaemonFragment(daemon, i);
        }
        
    	// Enregisterment de la liste contenant l'ordre d'envoi des fragments dans les noeuds
    	FileRegisterI.save(register);
    }
    
    private void hdfsRead(FileDescriptionI file) throws IOException {
    	if (register.hasData(file)) {
    		FileDataI data = register.getData(file);
    		int numberDaemons = data.getNumberDaemons();
    		if (numberDaemons > 0) {
	    		this.clientStream.sendData(numberDaemons);
		        
		    	// Envoi des informations des nodes au client
		    	Iterator<Integer> iterator = data.iterator();
		        String[] hostNames = new String[numberDaemons];
		        int[] ports = new int[numberDaemons];
		        int[] daemons = new int[ClusterConfig.nbMachine];
		        for (int i = 0; i < numberDaemons; i++) {
		        	int daemon = iterator.next();
		        	daemons[daemon] = i;
		        	hostNames[i] = ClusterConfig.nomMachine[daemon];
		        	ports[i] = ClusterConfig.numPortHDFS[daemon];
		        }
		    	this.sendClusterData(numberDaemons, hostNames, ports, ClusterConfig.redundancy);
		    	
		    	// Envoi des fragments du fichier des noeuds au client
				for (Integer daemon : data) {
					Collection<Integer> fragments = data.getDaemonFragments(daemon);
					this.clientStream.sendData(daemons[daemon]);
					this.clientStream.sendData(fragments.size());
					for (Integer fragment : fragments) 
						this.clientStream.sendData(fragment);
				}
    		}
    	}
    }
  
	private void hdfsDelete(FileDescriptionI file) throws IOException {
	    if (register.hasData(file)) {
	            
			// Suppression des données du fichier du register
			FileDataI data = register.removeData(file);

	        Cluster cluster = getClusterFile(data);
    		if (cluster != null) {
				cluster.connectAll();
				cluster.sendAllData(Command.Delete);
				cluster.sendAllData(file);
				cluster.closeAll();
    		}
		}
	}

	private void hdfsUpdate(FileDescriptionI file) throws IOException {
	    if (register.hasData(file)) {
			FileDataI data = register.getData(file);
    		data.getFile().update(file);
	        Cluster cluster = getClusterFile(data);
    		if (cluster != null) {
    			cluster.connectAll();
    			cluster.sendAllData(Command.Update);
    			cluster.sendAllData(file);
    			cluster.closeAll();
    		}
    	}
	}
	
	private void hdfsStatus() throws IOException {
		this.clientStream.sendData(register.getNumberFiles());
		for (FileDescriptionI file : register) {
			this.clientStream.sendData(file);
		}
	}
    
    private static Cluster getClusterFile(FileDataI data) {
    	Cluster cluster = null;
    	int numberDaemons = data.getNumberDaemons();
    	if (numberDaemons > 0) {
	        String[] hostNames = new String[numberDaemons];
	        int[] ports = new int[numberDaemons];
	        Iterator<Integer> iterator = data.iterator();
	        for (int i = 0; i < numberDaemons; i++) {
	        	int daemon = iterator.next();
	        	hostNames[i] = ClusterConfig.nomMachine[daemon];
	        	ports[i] = ClusterConfig.numPortHDFS[daemon];
	        }
	        cluster = new Cluster(numberDaemons, hostNames, ports, ClusterConfig.redundancy);
    	}
    	return cluster;
    }
	
}
