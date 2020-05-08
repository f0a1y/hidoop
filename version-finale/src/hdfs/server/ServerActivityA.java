package hdfs.server;

import java.io.IOException;
import java.net.Socket;

import config.ClusterConfig;
import hdfs.ActivityI;
import hdfs.Command;
import hdfs.CommunicationStream;
import hdfs.FileDescriptionI;

public abstract class ServerActivityA implements ActivityI {

    static FileRegisterI register;
    private Socket client;
    protected CommunicationStream clientStream;
	
	public ServerActivityA(Socket client) {
		this.client = client;
	}

	@Override
	public void start() throws IOException {
		if ((register = FileRegisterI.open()) == null) 
			register = ClusterConfig.getFileRegister();
        this.clientStream = new CommunicationStream(client);
        
        // Réception de la commande
        Command command = this.clientStream.receiveCommand(); 
    	
        // Réception du nom du fichier
        FileDescriptionI file = null;
        if (command.isFileCommand())	
        	file = this.clientStream.receiveFile();
        
        this.execute(command, file);
	}

	@Override
	public void terminate() throws IOException {
		this.client.close();
		this.clientStream.close();
	}

}
