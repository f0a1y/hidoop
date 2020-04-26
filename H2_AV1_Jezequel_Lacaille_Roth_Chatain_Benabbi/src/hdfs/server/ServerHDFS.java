package hdfs.server;

import java.net.ServerSocket;
import java.net.Socket;

import config.ClusterConfig;
import config.GeneralConfig;
import hdfs.ActivityI;

public class ServerHDFS extends Thread {

    private Socket client;

    public ServerHDFS(Socket client) {
        super();
        this.client = client;
    }

    public void run() {
        try {
            ActivityI activity = ClusterConfig.getServerActivity(this.client);
            activity.start();
	    	activity.terminate();
        } catch (Exception e) {e.printStackTrace();}
    }

    public static void main(String[] args) {
    	try {

    		// Attente d'un client
    		@SuppressWarnings("resource")
			ServerSocket mainServer = new ServerSocket(GeneralConfig.port);
    		while (true) {
    			ServerHDFS server = new ServerHDFS(mainServer.accept());
    			server.start();
    		}
    		
    	} catch (Exception e) {e.printStackTrace();}
    }

} 
