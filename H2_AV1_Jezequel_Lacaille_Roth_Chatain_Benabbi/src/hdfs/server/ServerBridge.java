package hdfs.server;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import config.ClusterConfig;
import formats.FormatWriter;
import formats.KV;
import hdfs.Command;
import hdfs.FileDescriptionI;
import hdfs.daemon.FragmentDataI;

public class ServerBridge {

    static FileRegisterI register;

    public static FileDataI getFileData(FileDescriptionI file) {
    	FileDataI data = null;
    	try {
    		if ((register = FileRegisterI.open()) != null) {
	    		if (register.hasData(file)) {
	    			data = register.getData(file);
	    		}
    		}
    	} catch (Exception e) {e.printStackTrace();}
    	return data;
    }
    
    public static HashMap<Integer, FragmentDataI> getFragmentData(FileDescriptionI file) {
    	HashMap<Integer, FragmentDataI> result = null;
    	try {
    		if ((register = FileRegisterI.open()) != null) {
	    		if (register.hasData(file)) {
	    			FileDataI fileData = register.getData(file);
	    			Cluster cluster = getClusterFile(fileData);
	        		if (cluster != null) {
		    			cluster.connectAll();
		    			cluster.sendAllData(Command.StatusFile);
		    			cluster.sendAllData(file);

		        		// Réception des numéros des fragments de chaque noeud
		    			Set<Integer> fragments = new HashSet<>();
		    			result = new HashMap<>();
		    			Iterator<Integer> iteratorDaemon = fileData.iterator();
		    			for (int i = 0; i < fileData.getNumberDaemons(); i++) {
		    				int daemon = iteratorDaemon.next();
			    			FragmentDataI fragmentData = cluster.receiveFragmentData(i);
		        			if (fragmentData!= null) {
		        				Iterator<Integer> iteratorFragment = fragmentData.iterator();
				    			while (iteratorFragment.hasNext()) {
				    				int fragment = iteratorFragment.next();
				    				if (fragments.contains(fragment))
				    					iteratorFragment.remove();
				    				else
				    					fragments.add(fragment);
				    			}
				    			if (fragmentData.getNumberFragments() > 0)	
				    				result.put(daemon, fragmentData);
		        			}
		    			}
		    			cluster.closeAll();
		    			
	        		}
	    		}
    		}
    	} catch (Exception e) {e.printStackTrace();}
    	return result;
    }
    
    public static void writeFragments(FileDescriptionI file, 	
    								  Collection<Integer> daemons, 
    								  String resultRepertory, 
    								  FormatWriter writer) {
    	try {
			if ((register = FileRegisterI.open()) != null) {
		    	if (register.hasData(file)) {
		    		FileDataI data = register.getData(file);
	    			Cluster cluster = getClusterFile(data);
	        		if (cluster != null) {
		    			cluster.connectAll(daemons);
		    			cluster.sendAllData(daemons, Command.getResult);
		    			cluster.sendAllData(daemons, file);
		    			cluster.sendAllData(daemons, resultRepertory);
	
		        		// Réception des fragments du fichier
		        		for (Integer daemon : daemons) {
		        			while (cluster.receiveDataByte(daemon) == 0) {
		        				String k = cluster.receiveDataString(daemon);
		        				String v = cluster.receiveDataString(daemon);
		        				writer.write(new KV(k, v));
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
	        	hostNames[i] = ClusterConfig.hosts[daemon];
	        	ports[i] = ClusterConfig.hdfsPorts[daemon];
	        }
	        cluster = new Cluster(numberDaemons, hostNames, ports, ClusterConfig.redundancy);
    	}
    	return cluster;
    }
    
}
