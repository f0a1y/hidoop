package hdfs.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import config.ClusterConfig;
import formats.KV;
import hdfs.Command;
import hdfs.FileDescriptionI;
import hdfs.daemon.FragmentDataI;
import ordo.SynchronizedList;

public class ServerLink extends Thread  {
	
    static FileRegisterI register = FileRegisterI.open();

    private Set<Integer> daemonIds;
    private SynchronizedList<Integer> daemonChannel;
    private SynchronizedList<KV> serverChannel;
    
    public ServerLink(Set<Integer> daemonIds,
    				  SynchronizedList<Integer> daemonChannel,
    				  SynchronizedList<KV> serverChannel) {
    	this.daemonIds = daemonIds;
    	this.daemonChannel = daemonChannel;
    	this.serverChannel = serverChannel;
    	this.serverChannel.beginInput();
    }
    
    @Override
    public void run() {
    	try {
			Cluster cluster = new Cluster(ClusterConfig.numberDaemons, 
										  ClusterConfig.hosts, 
										  ClusterConfig.ports[ClusterConfig.link], 
										  ClusterConfig.redundancy);
    		if (cluster != null) {
    			cluster.connectAll(this.daemonIds);
    	    	List<Integer> daemons = new ArrayList<>();
    	    	while (this.daemonChannel.waitUntilIsNotEmpty()) {
    	    		this.daemonChannel.removeAllInto(100, daemons);
    	    		for (Integer daemon : daemons) {
    	    			int numberKV = cluster.receiveDataInt(daemon);
    	    			for (int i = 0; i < numberKV; i++)
    	    				this.serverChannel.add(cluster.receiveKV(daemon));
    	    		}
    	    		daemons.clear();
    	    	}
    	    	cluster.closeAll(this.daemonIds);
    		}
			this.serverChannel.endInput();
    	} catch (Exception e) {e.printStackTrace();}
    }
    
    public static FileDataI getFileData(FileDescriptionI file) {
    	FileDataI data = null;
    	try {
    		if (register != null && register.hasData(file)) {
    			data = register.getData(file);
    		}
    	} catch (Exception e) {e.printStackTrace();}
    	return data;
    }
    
    public static HashMap<Integer, FragmentDataI> getFragmentData(FileDescriptionI file) {
    	HashMap<Integer, FragmentDataI> result = null;
    	try {
    		if (register != null && register.hasData(file)) {
    			FileDataI fileData = register.getData(file);
    			Cluster cluster = getClusterFile(fileData, ClusterConfig.hdfs);
        		if (cluster != null) {
	    			cluster.connectAll();
	    			cluster.sendAllData(Command.StatusFile);
	    			cluster.sendAllData(file);

	        		// Réception des numéros des fragments de chaque noeud
	    			HashMap<Integer, List<Integer>> fragments = new HashMap<>();
	    			for (int i = 0; i < fileData.getNumberFragments(); i++)
	    				fragments.put(i, new ArrayList<>());
	    			result = new HashMap<>();
	    			Iterator<Integer> iteratorDaemon = fileData.iterator();
	    			for (int i = 0; i < fileData.getNumberDaemons(); i++) {
	    				int daemon = iteratorDaemon.next();
		    			FragmentDataI fragmentData = cluster.receiveFragmentData(i);
	        			if (fragmentData!= null) {
	        				result.put(daemon, fragmentData);
	        				for (Integer fragment : fragmentData)
	        					fragments.get(fragment).add(daemon);
	        			}
	    			}
	    			cluster.closeAll();
	    			for (int i = 0; i < fileData.getNumberFragments(); i++) {
	    				List<Integer> daemons = fragments.get(i);
	    				daemons.remove(i % daemons.size());
	    				for (Integer daemon : daemons) 
	    					result.get(daemon).removeFragment(i);
	    			}
	    			for (Integer daemon : fileData) 
	    				if (result.get(daemon).getNumberFragments() == 0)
	    					result.remove(daemon);
        		}
    		}
    	} catch (Exception e) {e.printStackTrace();}
    	return result;
    }
    
    private static Cluster getClusterFile(FileDataI data, int service) {
    	Cluster cluster = null;
    	int numberDaemons = data.getNumberDaemons();
    	if (numberDaemons > 0) {
	        String[] hostNames = new String[numberDaemons];
	        int[] ports = new int[numberDaemons];
	        Iterator<Integer> iterator = data.iterator();
	        for (int i = 0; i < numberDaemons; i++) {
	        	int daemon = iterator.next();
	        	hostNames[i] = ClusterConfig.hosts[daemon];
	        	ports[i] = ClusterConfig.ports[service][daemon];
	        }
	        cluster = new Cluster(numberDaemons, hostNames, ports, ClusterConfig.redundancy);
    	}
    	return cluster;
    }
    
}
