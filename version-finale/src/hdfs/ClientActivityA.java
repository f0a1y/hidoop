package hdfs;

import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;

import config.GeneralConfig;
import hdfs.fileProcessing.FileBreakerSelectorI;

public abstract class ClientActivityA implements ActivityI {

	private Socket server;
	protected CommunicationStream serverStream;
	protected FileBreakerSelectorI selector;
	private List<List<Command>> commands;
	private List<List<FileDescriptionI>> files;
	
	public ClientActivityA(FileBreakerSelectorI selector, List<List<Command>> commands, List<List<FileDescriptionI>> files) {
		this.selector = selector;
		this.commands = commands;
		this.files = files;
	}
	
	@Override
	public void start() throws IOException {
		// Connexion avec le server
        this.server = new Socket(GeneralConfig.host, GeneralConfig.port);
        this.serverStream = new CommunicationStream(this.server);
        Iterator<List<Command>> commandGroupIterator = this.commands.iterator();
        Iterator<List<FileDescriptionI>> fileGroupIterator = this.files.iterator();
		while (commandGroupIterator.hasNext() && fileGroupIterator.hasNext()) {
			Iterator<Command> commandIterator = commandGroupIterator.next().iterator();
			while (commandIterator.hasNext()) {
				Command command = commandIterator.next();
				if (command.isFileCommand()) {
					Iterator<FileDescriptionI> fileIterator = fileGroupIterator.next().iterator();
					while (fileIterator.hasNext()) {
						FileDescriptionI file = fileIterator.next();
						// Envoie de la commande
						this.serverStream.sendData(command);
						
						// Envoie de la description du fichier
						this.serverStream.sendData(file);

				        this.execute(command, file);
					}
				} else {

					// Envoie de la commande
					this.serverStream.sendData(command);
			        
			        this.execute(command, null);
				}
			}
		}
	}
	
	@Override
	public void terminate() throws IOException {
		this.serverStream.close();
		this.server.close();
	}

}
