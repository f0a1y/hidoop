package hdfs.daemon;

import java.net.ServerSocket;
import java.net.Socket;
import config.ClusterConfig;
import hdfs.ActivityI;

public class DaemonHDFS extends Thread {

    private int id;
    private Socket emitter;
    
    public DaemonHDFS(Socket emitter, int id) {
        super();
        this.emitter = emitter;
        this.id = id;
    }
    
    public void run() {
        try {
            ActivityI activity = ClusterConfig.getDaemonActivity(this.emitter, this.id);
            activity.start();
	    	activity.terminate();
        } catch (Exception e) {e.printStackTrace();}
    }

    public static void main(String[] args) {
        try {
        	if (args.length == 1) {
	            int id = Integer.parseInt(args[0]);
	            @SuppressWarnings("resource")
				ServerSocket client = new ServerSocket(ClusterConfig.hdfsPorts[id]);
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
