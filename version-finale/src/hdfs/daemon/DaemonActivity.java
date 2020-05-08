package hdfs.daemon;

import java.io.IOException;
import java.net.Socket;

import formats.Format;
import formats.KV;
import formats.Format.OpenMode;
import formats.FormatSelectorI;
import hdfs.Command;
import hdfs.FileDescriptionI;

public class DaemonActivity extends DaemonActivityA {
	
	public DaemonActivity(Socket emitter, FormatSelectorI selector, int id) {
		super(emitter, selector, id);
	}

	@Override
	public boolean execute(Command command, FileDescriptionI file) throws IOException {
	    boolean result = true;
        if (command == Command.Upload) {
        	this.hdfsWrite(file);
        } else if (command == Command.Download) {
        	this.hdfsRead(file);
        } else if (command == Command.Delete) {
        	this.hdfsDelete(file);
    	} else if (command == Command.Update) {
        	this.hdfsUpdate(file);
    	} else if (command == Command.StatusFile) {
        	this.hdfsStatusFile(file);
    	} else {
	   		result = false;
	   	}
		return result;
	}
    
    private void hdfsWrite(FileDescriptionI file) throws IOException {
        if (this.emitterStream.receiveDataByte() > 0) {

        	// Suppression des fragments préexistants
        	this.hdfsDelete(file);
        	
        	// Liste de numéros des fragments du file
        	register.addData(file, this.id);
        	
    	    // Dossier des fragments du file
    	    String repertoryName = register.createRepertory(file);
    	    
            Integer order = this.emitterStream.receiveDataInt();
    		do {
    			String fragmentName = register.createFragment(file, order);
	            
	            // Récupération de l'éditeur adapté au file
	            Format editeur = selector.selectFormat(repertoryName + fragmentName);
				editeur.open(OpenMode.W);
	            
	            // Réception du fragment
	            do {
		            String fragment = new String(this.emitterStream.receiveData());
		            editeur.write(new KV(order.toString(), fragment));
	            } while (this.emitterStream.receiveDataByte() == 0);
				editeur.close();
    		} while ((order = this.emitterStream.receiveDataInt()) >= 0);
        	FragmentRegisterI.save(register, this.id);
        }
    }
    
    private void hdfsRead(FileDescriptionI file) throws IOException {
    	if (register.hasData(file)) {
    		FragmentDataI data = register.getData(file);
    		
    		this.hdfsStatusFile(file);
			
    	    // Dossier des fragments du file
    	    String repertoryName = data.getFragmentsPath();
    	    
			while (this.emitterStream.receiveDataByte() == 0) {
				
				int fragment = this.emitterStream.receiveDataInt();
				String fragmentName = data.getFragmentName(fragment);
				Format lecteur = this.selector.selectFormat(repertoryName + fragmentName);
			    
				if (lecteur.open(OpenMode.R)) {
					KV pair;
					while ((pair = lecteur.read()) != null) {
						this.emitterStream.sendData((byte)0);
						this.emitterStream.sendData(pair.k);
						this.emitterStream.sendData(pair.v);
					}
					lecteur.close();
				}
				this.emitterStream.sendData((byte)1);
			}
    	}
    }
    
    private void hdfsDelete(FileDescriptionI file) {
    	if (register.deleteData(file))
        	FragmentRegisterI.save(register, this.id);
    }
    
    private void hdfsUpdate(FileDescriptionI file) throws IOException {
    	if (register.updateData(file, this.id)) 
        	FragmentRegisterI.save(register, this.id);
    }
    
    private void hdfsStatusFile(FileDescriptionI file) throws IOException {
    	if (register.hasData(file)) {
    		this.emitterStream.sendData(register.getData(file));
    	}
    }

}
