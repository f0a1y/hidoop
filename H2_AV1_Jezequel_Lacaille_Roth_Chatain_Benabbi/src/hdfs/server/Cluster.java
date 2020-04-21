package hdfs.server;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import hdfs.Command;
import hdfs.CommunicationStream;
import hdfs.FileDescriptionI;
import hdfs.daemon.FragmentDataI;

public class Cluster {

	private int numberDaemons;
	private String[] hosts;
	private int[] ports;
	private int redundancy;
	private Socket[] daemons;
	private CommunicationStream[] daemonStreams;
	
	public Cluster(int numberNodes, String[] hosts, int[] ports, int redundancy) {
		this.numberDaemons = numberNodes;
		this.hosts = hosts;
		this.ports = ports;
		this.redundancy = redundancy;
		this.daemons = new Socket[this.numberDaemons];
		this.daemonStreams = new CommunicationStream[this.numberDaemons];
	}

	public void connectAll() throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) {
			this.daemons[i] = new Socket(this.hosts[i], this.ports[i]);
			this.daemonStreams[i] = new CommunicationStream(this.daemons[i]);
		}
	}

	public void connectAll(Collection<Integer> daemons) throws IOException {
		for (Integer daemon : daemons) {
			this.daemons[daemon] = new Socket(this.hosts[daemon], this.ports[daemon]);
			this.daemonStreams[daemon] = new CommunicationStream(this.daemons[daemon]);
		}
	}

	public void connectAll(int[] daemons) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemons[daemons[i]] = new Socket(this.hosts[daemons[i]], this.ports[daemons[i]]);
			this.daemonStreams[daemons[i]] = new CommunicationStream(this.daemons[daemons[i]]);
		}
	}

	public void connect(int daemon) throws IOException {
		this.daemons[daemon] = new Socket(this.hosts[daemon], this.ports[daemon]);
		this.daemonStreams[daemon] = new CommunicationStream(this.daemons[daemon]);
	}

	public void connectAllExcept(Collection<Integer> daemons) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i)) {
				this.daemons[i] = new Socket(this.hosts[i], this.ports[i]);
				this.daemonStreams[i] = new CommunicationStream(this.daemons[i]);
			}
	}

	public void connectAllExcept(int[] daemons) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) {
				this.daemons[i] = new Socket(this.hosts[i], this.ports[i]);
				this.daemonStreams[i] = new CommunicationStream(this.daemons[i]);
			}
	}

	public void connectAllExcept(int daemon) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon) {
				this.daemons[i] = new Socket(this.hosts[i], this.ports[i]);
				this.daemonStreams[i] = new CommunicationStream(this.daemons[i]);
			}
	}
	
	public void sendClusterData(CommunicationStream receiver) throws IOException {
		// Envoi du nombre de noeuds au récepteur
		receiver.sendData(this.numberDaemons);
    	
    	// Envoi des informations de tous les noeuds au récepteur
    	byte[] buffer;
    	for (int i = 0; i < this.numberDaemons; i++) {
    		
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

	public int getNumberDaemons() {
		return this.numberDaemons;
	}
	
	public int getRedundancy() {
		return this.redundancy;
	}
	
	public FileDescriptionI receiveFile(int daemon) throws IOException {
		return this.daemonStreams[daemon].receiveFile();
	}
	
	public FileDataI receiveFileData(int daemon) throws IOException {
		return this.daemonStreams[daemon].receiveFileData();
	}
	
	public FragmentDataI receiveFragmentData(int daemon) throws IOException {
		return this.daemonStreams[daemon].receiveFragmentData();
	}
	
	public Command receiveCommand(int daemon) throws IOException {
		return this.daemonStreams[daemon].receiveCommand();
	}
	
	public byte[] receiveData(int daemon) throws IOException {
		return this.daemonStreams[daemon].receiveData();
	}
	
	public byte receiveDataByte(int daemon) throws IOException {
		return this.daemonStreams[daemon].receiveDataByte();
	}
	
	public String receiveDataString(int daemon) throws IOException {
		return this.daemonStreams[daemon].receiveDataString();
	}
	
	public int receiveDataInt(int daemon) throws IOException {
		return this.daemonStreams[daemon].receiveDataInt();
	}
	
	public long receiveDataLong(int daemon) throws IOException {
		return this.daemonStreams[daemon].receiveDataLong();
	}
	
	public float receiveDataFloat(int daemon) throws IOException {
		return this.daemonStreams[daemon].receiveDataFloat();
	}
	
	public double receiveDataDouble(int daemon) throws IOException {
		return this.daemonStreams[daemon].receiveDataDouble();
	}
	
	public void sendAllData(FileDescriptionI file) throws IOException {
		for (int i = 0; i < this.daemonStreams.length; i++) {
			this.daemonStreams[i].sendData(file);
		}
	}
	
	public void sendAllData(Collection<Integer> daemons, FileDescriptionI file) throws IOException {
		for (Integer daemon : daemons) {
			this.daemonStreams[daemon].sendData(file);
		}
	}
	
	public void sendAllData(int[] daemons, FileDescriptionI file) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemonStreams[daemons[i]].sendData(file);
		}
	}
	
	public void sendData(int daemon, FileDescriptionI file) throws IOException {
		this.daemonStreams[daemon].sendData(file);
	}
	
	public void sendAllDataExcept(Collection<Integer> daemons, FileDescriptionI file) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i))
				this.daemonStreams[i].sendData(file);
	}
	
	public void sendAllDataExcept(int[] daemons, FileDescriptionI file) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) 
				this.daemonStreams[i].sendData(file);
	}
	
	public void sendAllDataExcept(int daemon, FileDescriptionI file) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon)
				this.daemonStreams[i].sendData(file);
	}

	public void sendAllData(FileDataI data) throws IOException {
		for (int i = 0; i < this.daemonStreams.length; i++) {
			this.daemonStreams[i].sendData(data);
		}
	}
	
	public void sendAllData(Collection<Integer> daemons, FileDataI data) throws IOException {
		for (Integer daemon : daemons) {
			this.daemonStreams[daemon].sendData(data);
		}
	}
	
	public void sendAllData(int[] daemons, FileDataI data) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemonStreams[daemons[i]].sendData(data);
		}
	}
	
	public void sendData(int daemon, FileDataI data) throws IOException {
		this.daemonStreams[daemon].sendData(data);
	}
	
	public void sendAllDataExcept(Collection<Integer> daemons, FileDataI data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i))
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int[] daemons, FileDataI data) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) 
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int daemon, FileDataI data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon)
				this.daemonStreams[i].sendData(data);
	}

	public void sendAllData(FragmentDataI data) throws IOException {
		for (int i = 0; i < this.daemonStreams.length; i++) {
			this.daemonStreams[i].sendData(data);
		}
	}
	
	public void sendAllData(Collection<Integer> daemons, FragmentDataI data) throws IOException {
		for (Integer daemon : daemons) {
			this.daemonStreams[daemon].sendData(data);
		}
	}
	
	public void sendAllData(int[] daemons, FragmentDataI data) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemonStreams[daemons[i]].sendData(data);
		}
	}
	
	public void sendData(int daemon, FragmentDataI data) throws IOException {
		this.daemonStreams[daemon].sendData(data);
	}	
	
	public void sendAllDataExcept(Collection<Integer> daemons, FragmentDataI data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i))
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int[] daemons, FragmentDataI data) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) 
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int daemon, FragmentDataI data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon)
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllData(Command command) throws IOException {
		for (int i = 0; i < this.daemonStreams.length; i++) {
			this.daemonStreams[i].sendData(command);
		}
	}
	
	public void sendAllData(Collection<Integer> daemons, Command command) throws IOException {
		for (Integer daemon : daemons) {
			this.daemonStreams[daemon].sendData(command);
		}
	}
	
	public void sendAllData(int[] daemons, Command command) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemonStreams[daemons[i]].sendData(command);
		}
	}
	
	public void sendData(int daemon, Command command) throws IOException {
		this.daemonStreams[daemon].sendData(command);
	}
	
	public void sendAllDataExcept(Collection<Integer> daemons, Command command) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i))
				this.daemonStreams[i].sendData(command);
	}
	
	public void sendAllDataExcept(int[] daemons, Command command) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) 
				this.daemonStreams[i].sendData(command);
	}
	
	public void sendAllDataExcept(int daemon, Command command) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon)
				this.daemonStreams[i].sendData(command);
	}
	
	public void sendAllData(byte[] data, int length) throws IOException {
		for (int i = 0; i < this.daemonStreams.length; i++) {
			this.daemonStreams[i].sendData(data, length);
		}
	}
	
	public void sendAllData(Collection<Integer> daemons, byte[] data, int length) throws IOException {
		for (Integer daemon : daemons) {
			this.daemonStreams[daemon].sendData(data, length);
		}
	}
	
	public void sendAllData(int[] daemons, byte[] data, int length) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemonStreams[daemons[i]].sendData(data, length);
		}
	}
	
	public void sendData(int daemon, byte[]data, int length) throws IOException {
		this.daemonStreams[daemon].sendData(data, length);
	}
	
	public void sendAllDataExcept(Collection<Integer> daemons, byte[] data, int length) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i))
				this.daemonStreams[i].sendData(data, length);
	}
	
	public void sendAllDataExcept(int[] daemons, byte[] data, int length) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) 
				this.daemonStreams[i].sendData(data, length);
	}
	
	public void sendAllDataExcept(int daemon, byte[] data, int length) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon)
				this.daemonStreams[i].sendData(data, length);
	}
	
	public void sendAllData(byte data) throws IOException {
		for (int i = 0; i < this.daemonStreams.length; i++) {
			this.daemonStreams[i].sendData(data);
		}
	}
	
	public void sendAllData(Collection<Integer> daemons, byte data) throws IOException {
		for (Integer daemon : daemons) {
			this.daemonStreams[daemon].sendData(data);
		}
	}
	
	public void sendAllData(int[] daemons, byte data) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemonStreams[daemons[i]].sendData(data);
		}
	}
	
	public void sendData(int daemon, byte data) throws IOException {
		this.daemonStreams[daemon].sendData(data);
	}
	
	public void sendAllDataExcept(Collection<Integer> daemons, byte data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i))
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int[] daemons, byte data) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) 
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int daemon, byte data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon)
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllData(String data) throws IOException {
		for (int i = 0; i < this.daemonStreams.length; i++) {
			this.daemonStreams[i].sendData(data);
		}
	}
	
	public void sendAllData(Collection<Integer> daemons, String data) throws IOException {
		for (Integer daemon : daemons) {
			this.daemonStreams[daemon].sendData(data);
		}
	}
	
	public void sendAllData(int[] daemons, String data) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemonStreams[daemons[i]].sendData(data);
		}
	}
	
	public void sendData(int daemon, String data) throws IOException {
		this.daemonStreams[daemon].sendData(data);
	}
	
	public void sendAllDataExcept(Collection<Integer> daemons, String data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i))
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int[] daemons, String data) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) 
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int daemon, String data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon)
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllData(int data) throws IOException {
		for (int i = 0; i < this.daemonStreams.length; i++) {
			this.daemonStreams[i].sendData(data);
		}
	}
	
	public void sendAllData(Collection<Integer> daemons, int data) throws IOException {
		for (Integer daemon : daemons) {
			this.daemonStreams[daemon].sendData(data);
		}
	}
	
	public void sendAllData(int[] daemons, int data) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemonStreams[daemons[i]].sendData(data);
		}
	}
	
	public void sendData(int daemon, int data) throws IOException {
		this.daemonStreams[daemon].sendData(data);
	}
	
	public void sendAllDataExcept(Collection<Integer> daemons, int data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i))
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int[] daemons, int data) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) 
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int daemon, int data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon)
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllData(long data) throws IOException {
		for (int i = 0; i < this.daemonStreams.length; i++) {
			this.daemonStreams[i].sendData(data);
		}
	}
	
	public void sendAllData(Collection<Integer> daemons, long data) throws IOException {
		for (Integer daemon : daemons) {
			this.daemonStreams[daemon].sendData(data);
		}
	}
	
	public void sendAllData(int[] daemons, long data) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemonStreams[daemons[i]].sendData(data);
		}
	}
	
	public void sendData(int daemon, long data) throws IOException {
		this.daemonStreams[daemon].sendData(data);
	}
	
	public void sendAllDataExcept(Collection<Integer> daemons, long data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i))
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int[] daemons, long data) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) 
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int daemon, long data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon)
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllData(float data) throws IOException {
		for (int i = 0; i < this.daemonStreams.length; i++) {
			this.daemonStreams[i].sendData(data);
		}
	}
	
	public void sendAllData(Collection<Integer> daemons, float data) throws IOException {
		for (Integer daemon : daemons) {
			this.daemonStreams[daemon].sendData(data);
		}
	}
	
	public void sendAllData(int[] daemons, float data) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemonStreams[daemons[i]].sendData(data);
		}
	}
	
	public void sendData(int daemon, float data) throws IOException {
		this.daemonStreams[daemon].sendData(data);
	}
	
	public void sendAllDataExcept(Collection<Integer> daemons, float data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i))
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int[] daemons, float data) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) 
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int daemon, float data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon)
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllData(double data) throws IOException {
		for (int i = 0; i < this.daemonStreams.length; i++) {
			this.daemonStreams[i].sendData(data);
		}
	}
	
	public void sendAllData(Collection<Integer> daemons, double data) throws IOException {
		for (Integer daemon : daemons) {
			this.daemonStreams[daemon].sendData(data);
		}
	}
	
	public void sendAllData(int[] daemons, double data) throws IOException {
		for (int i = 0; i < daemons.length; i++) {
			this.daemonStreams[daemons[i]].sendData(data);
		}
	}
	
	public void sendData(int daemon, double data) throws IOException {
		this.daemonStreams[daemon].sendData(data);
	}
	
	public void sendAllDataExcept(Collection<Integer> daemons, double data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i))
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int[] daemons, double data) throws IOException {
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) 
				this.daemonStreams[i].sendData(data);
	}
	
	public void sendAllDataExcept(int daemon, double data) throws IOException {
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon)
				this.daemonStreams[i].sendData(data);
	}
	
	public void closeAll() throws IOException{
		for (int i = 0; i < this.numberDaemons; i++) {
			this.daemons[i].close();
			this.daemonStreams[i].close();
		}
	}
	
	public void closeAll(Collection<Integer> daemons) throws IOException{
		for (int daemon : daemons) {
			this.daemons[daemon].close();
			this.daemonStreams[daemon].close();
		}
	}
	
	public void closeAll(int[] daemons) throws IOException{
		for (int daemon : daemons) {
			this.daemons[daemon].close();
			this.daemonStreams[daemon].close();
		}
	}
	
	public void close(int daemon) throws IOException{
		this.daemons[daemon].close();
		this.daemonStreams[daemon].close();
	}
	
	public void closeAllExcept(Collection<Integer> daemons) throws IOException{
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemons.contains(i)) {
				this.daemons[i].close();
				this.daemonStreams[i].close();
			}
	}
	
	public void closeAllExcept(int[] daemons) throws IOException{
		List<Integer> daemonsIgnored = new ArrayList<>();
		for (int daemon : daemons)
			daemonsIgnored.add(daemon);
		for (int i = 0; i < this.numberDaemons; i++) 
			if (!daemonsIgnored.contains(i)) {
				this.daemons[i].close();
				this.daemonStreams[i].close();
			}
	}
	
	public void closeAllExcept(int daemon) throws IOException{
		for (int i = 0; i < this.numberDaemons; i++) 
			if (i != daemon) {
				this.daemons[i].close();
				this.daemonStreams[i].close();
			}
	}
	
}
