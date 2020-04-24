package hdfs.daemon;

import java.io.IOException;
import java.net.Socket;

import config.ClusterConfig;
import formats.FormatSelectorI;
import hdfs.ActivityI;
import hdfs.Command;
import hdfs.CommunicationStream;
import hdfs.FileDescriptionI;

public abstract class DaemonActivityA implements ActivityI {

    static FragmentRegisterI register;
    private Socket emitter;
    protected CommunicationStream emitterStream;
    protected FormatSelectorI selector;
    protected int id;
	
	public DaemonActivityA(Socket emitter, FormatSelectorI selector, int id) {
		this.emitter = emitter;
		this.selector = selector;
		this.id = id;
	}

	@Override
	public void start() throws IOException {
		if ((register = FragmentRegisterI.open(this.id)) == null) 
			register = ClusterConfig.getFragmentRegister();
        this.emitterStream = new CommunicationStream(emitter);
        
        // Réception de la commande
        Command command = this.emitterStream.receiveCommand(); 
    	
        // Réception du nom du fichier
        FileDescriptionI file = null;
        if (command.isFileCommand())	
        	file = this.emitterStream.receiveFile();
        
        this.execute(command, file);
	}

	@Override
	public void terminate() throws IOException {
		this.emitter.close();
		this.emitterStream.close();
	}

}
