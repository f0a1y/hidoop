package hdfs.daemon;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import config.ClusterConfig;
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
        	
        	// Liste de numéros des fragments du file
        	FragmentDataI data = register.addData(file);
    		
        	// Suppression des fragments préexistants
        	this.hdfsDelete(file);
        	
    	    // Dossier des fragments du file
    	    String repertoryName = ClusterConfig.getDataPath() + data.getFragmentsPath();
    	    File repertory = new File(repertoryName);
    	    repertory.mkdir();
    	    
            Integer order = this.emitterStream.receiveDataInt();
    		do {
	            String fragmentName = ClusterConfig.fragmentToName(order);
	            data.addFragment(order, fragmentName);
	            
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
    		
			// Réception des numéros des fragments à envoyer
    	    int numberFragments = this.emitterStream.receiveDataInt();
    	    List<Integer> fragments = new ArrayList<>();
			for (int i = 0; i < numberFragments; i++) {
				int fragment = this.emitterStream.receiveDataInt();
				if (data.hasFragment(fragment))
					fragments.add(fragment);
			}
			this.emitterStream.sendData(fragments.size());
			for (Integer fragment : fragments)
				this.emitterStream.sendData(fragment);
			fragments.clear();
			numberFragments = this.emitterStream.receiveDataInt();
			for (int i = 0; i < numberFragments; i++)
				fragments.add(this.emitterStream.receiveDataInt());
			
    	    // Dossier des fragments du file
    	    String repertoryName = ClusterConfig.getDataPath() + data.getFragmentsPath();
    	    
			for (int i = 0; i < numberFragments; i++) {
				
				// Récupération du lecteur adapté au format du fragment
				int fragmentNumber = this.emitterStream.receiveDataInt();
				String fragmentName = data.getFragmentName(fragmentNumber);
				Format lecteur = this.selector.selectFormat(repertoryName + fragmentName);
			    
				lecteur.open(OpenMode.R);
				KV fragment;
				while ((fragment = lecteur.read()) != null) {
					this.emitterStream.sendData((byte)0);
					this.emitterStream.sendData(fragment.k);
					this.emitterStream.sendData(fragment.v);
				}
				this.emitterStream.sendData((byte)1);
				lecteur.close();
			}
    	}
    }
    
    private void hdfsDelete(FileDescriptionI file) {
    	if (register.hasData(file)) {
    		FragmentDataI data = register.getData(file);
		    String repertoryName = ClusterConfig.getDataPath() + data.getFragmentsPath();
	    	File repertory = new File(repertoryName);
		    if (repertory.exists()) {
				for (File fragmentFile : repertory.listFiles()) {
					fragmentFile.delete();
				}
			}
	    	repertory.delete();
    	}
    }
    
    private void hdfsUpdate(FileDescriptionI file) throws IOException {
    	if (register.hasData(file)) {
    		FragmentDataI data = register.getData(file);
    		data.getFile().update(file);
    	}
    }
    
    private void hdfsStatusFile(FileDescriptionI file) throws IOException {
    	if (register.hasData(file)) {
    		this.emitterStream.sendData(register.getData(file));
    	}
    }

}
