package hdfs.daemon;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import config.ClusterConfig;
import formats.KV;
import hdfs.CommunicationStream;
import ordo.SynchronizedList;

public class DaemonLink extends Thread {

    private int id;
    private SynchronizedList<KV> hidoopChannel;
    private SynchronizedList<Integer> hdfsChannel;
    
    public DaemonLink(int id, SynchronizedList<KV> hidoopChannel, SynchronizedList<Integer> hdfsChannel) {
        super();
        this.id = id;
        this.hidoopChannel = hidoopChannel;
        this.hdfsChannel = hdfsChannel;
    	this.hdfsChannel.beginInput();
    }
    
    public void run() {
        try {
			ServerSocket daemon = new ServerSocket(ClusterConfig.ports[ClusterConfig.link][id]);
	        Socket emitter = daemon.accept();
	        CommunicationStream emitterStream = new CommunicationStream(emitter);
			daemon.close();
	    	List<KV> input = new ArrayList<>();
	    	while (this.hidoopChannel.waitUntilIsNotEmpty()) {
	    		this.hidoopChannel.removeAllInto(100, input);
	    		emitterStream.sendData(input.size());
		    	for (KV pair : input)
		    		emitterStream.sendData(pair);
		   		input.clear();
		       	this.hdfsChannel.add(this.id);
	    	}
	    	this.hdfsChannel.endInput();
			emitter.close();
			emitterStream.close();
        } catch (Exception e) {e.printStackTrace();}
    }

}
