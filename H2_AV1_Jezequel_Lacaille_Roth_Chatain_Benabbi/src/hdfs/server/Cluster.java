package hdfs.server;

import java.io.IOException;
import java.net.Socket;
import hdfs.CommunicationStream;

public class Cluster {

	private int numberNodes;
	private String[] hosts;
	private int[] ports;
	private Socket[] nodes;
	private CommunicationStream[] nodesStreams;
	private int redundancy;
	
	public Cluster(int numberNodes, String[] hosts, int[] ports, int redundancy) {
		this.numberNodes = numberNodes;
		this.hosts = hosts;
		this.ports = ports;
		this.redundancy = redundancy;
	}

	public void connect() throws IOException{
		this.nodes = new Socket[this.numberNodes];
		this.nodesStreams = new CommunicationStream[this.numberNodes];
		for (int i = 0; i < this.numberNodes; i++) {
			this.nodes[i] = new Socket(this.hosts[i], this.ports[i]);
			this.nodesStreams[i] = new CommunicationStream(this.nodes[i]);
		}
	}
	
	public void sendClusterData(CommunicationStream receiver) throws IOException {
		// Envoi du nombre de noeuds au récepteur
		receiver.sendData(this.numberNodes);
    	
    	// Envoi des informations de tous les noeuds au récepteur
    	byte[] buffer;
    	for (int i = 0; i < this.numberNodes; i++) {
    		
    		// Envoi du nom d'hôte du noeud au récepteur
    		buffer = this.hosts[i].getBytes();
    		receiver.sendData(buffer, buffer.length);
    		
    		// Envoi du port du noeud au récepteur
    		receiver.sendData(this.ports[i]);
    	}

    	// Envoi du nombre de redundancy au récepteur
    	receiver.sendData(this.redundancy);
	}
	
	public static Cluster receiveClusterData(CommunicationStream emitter) throws IOException {
		// Réception du nombre de noeuds du cluster 
        int numberNodes = emitter.receiveDataInt();
        String[] hosts = new String[numberNodes];
        int[] ports = new int[numberNodes];
    	
    	for (int i = 0; i < numberNodes; i++) {
            
            // Réception du nom d'hôte du noeud 
            hosts[i] = new String(emitter.receiveData());
            
            // Réception du port du noeud
            ports[i] = emitter.receiveDataInt();
    	}
    	
    	// Réception du nombre de redondance
        int redundancy = emitter.receiveDataInt();
        
        return new Cluster(numberNodes, hosts, ports, redundancy);
	}
	
	public byte[] receiveData(int daemon) throws IOException {
		return this.nodesStreams[daemon].receiveData();
	}
	
	public int receiveDataInt(int daemon) throws IOException {
		return this.nodesStreams[daemon].receiveDataInt();
	}
	
	public void sendAllData(int[] daemons, byte[] data, int length) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.nodesStreams[daemons[i]].sendData(data, length);
		}
	}
	
	public void sendData(int daemon, byte[]data, int length) throws IOException {
		this.nodesStreams[daemon].sendData(data, length);
	}
	
	public void sendAllData(int[] daemons, int data) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.nodesStreams[daemons[i]].sendData(data);
		}
	}
	
	public void sendData(int daemon, int data) throws IOException {
		this.nodesStreams[daemon].sendData(data);
	}

	public int getNumberNodes() {
		return this.numberNodes;
	}
	
	public int getRedundancy() {
		return this.redundancy;
	}
	
	public void close() throws IOException{
		for (int i = 0; i < this.numberNodes; i++) {
			this.nodes[i].close();
			this.nodesStreams[i].close();
		}
	}
	
}
