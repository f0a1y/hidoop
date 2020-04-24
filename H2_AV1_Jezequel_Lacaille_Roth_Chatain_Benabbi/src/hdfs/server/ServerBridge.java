package hdfs.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import config.ClusterConfig;
import formats.FormatWriter;
import formats.KV;
import hdfs.Command;
import hdfs.FileDescriptionI;
import hdfs.daemon.FragmentDataI;

public class ServerBridge {

    static FileRegisterI register;

    public static HashMap<Integer, FragmentDataI> getFileData(FileDescriptionI file) {
    	HashMap<Integer, FragmentDataI> result = null;
    	try {
    		if ((register = FileRegisterI.open()) != null) {
	    		if (register.hasData(file)) {
	    			FileDataI data = register.getData(file);
	    			Cluster cluster = getClusterFile(data);
	        		if (cluster != null) {
		    			cluster.connectAll();
		    			cluster.sendAllData(Command.StatusFile);
		    			cluster.sendAllData(file);

		        		// Réception des numéros des fragments de chaque noeud
		    			result = new HashMap<>();
		    			Iterator<Integer> iterator = data.iterator();
		    			for (int i = 0; i < data.getNumberDaemons(); i++) {
			    			FragmentDataI fragment = cluster.receiveFragmentData(i);
		        			if (fragment != null)
		        				result.put(iterator.next(), fragment);
		    			}
		    			
		    			cluster.closeAll();
	        		}
	    		}
    		}
    	} catch (Exception e) {e.printStackTrace();}
    	return result;
    }
    
    public static void recupererResultats(FileDescriptionI file, FormatWriter writer) {
    	try {
			if ((register = FileRegisterI.open()) != null) {
		    	if (register.hasData(file)) {
		    		FileDataI data = register.getData(file);
	    			Cluster cluster = getClusterFile(data);
	        		if (cluster != null) {
		    			cluster.connectAll();
		    			cluster.sendAllData(Command.Download);
		    			cluster.sendAllData(file);
	
		            	// Etablissement de l'ordre de réception des fragments
		        		int fragmentMax = 0;
		    			Iterator<Integer> iterator = data.iterator();
		    			for (int i = 0; i < data.getNumberDaemons(); i++) {
							Collection<Integer> fragments = data.getDaemonFragments(iterator.next());
							cluster.sendData(i, fragments.size());
							for (Integer fragment : fragments) {
			    				if (fragment > fragmentMax) 
			    					fragmentMax = fragment;
			    				cluster.sendData(i, fragment);
							}
						}
		        		int[] order = new int[fragmentMax];
		        		for (int i = 0; i < fragmentMax; i++)
		        			order[i] = -1;
		        		Set<Integer> daemonsUsed = new HashSet<>();
		        		for (int i = 0; i < data.getNumberDaemons(); i++) {
		        			int numberFragments = cluster.receiveDataInt(i);
		        			List<Integer> fragments = new ArrayList<>();
		        			for (int j = 0; j < numberFragments; j++) {
		        				int fragment = cluster.receiveDataInt(i);
		        				if (order[fragment] == -1) {
		        					order[fragment] = i;
		        					fragments.add(fragment);
		        					daemonsUsed.add(i);
		        				}
		        			}
		        			cluster.sendData(i, fragments.size());
		        			for (int fragment : fragments)
		        				cluster.sendData(i, fragment);
		        		}
		        		
		        		// Fermeture des noeuds non nécessaires
		        		cluster.closeAllExcept(daemonsUsed);
	
		        		// Réception des fragments du fichier
		        		for (int i = 0; i < fragmentMax; i++) {
		        			if (order[i] != -1) {
		        				cluster.sendData(order[i], i);
		        				while (cluster.receiveDataByte(order[i]) == 0) {
		        					String k = cluster.receiveDataString(order[i]);
		        					String v = cluster.receiveDataString(order[i]);
		        					writer.write(new KV(k, v));
		        				}
		        			}
		        		}
	        		}
		    	}
			}
    	} catch (Exception e) {e.printStackTrace();}
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
