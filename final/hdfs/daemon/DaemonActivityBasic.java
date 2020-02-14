package hdfs.daemon;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import config.ClusterConfig;
import formats.Format;
import formats.KV;
import formats.Format.OpenMode;
import formats.FormatSelectorI;
import hdfs.ActivityI;
import hdfs.CommunicationStream;
import hdfs.daemon.DaemonHDFS.FragmentFile;

public class DaemonActivityBasic implements ActivityI {
	
	protected CommunicationStream emitterStream;
    static HashMap<String, List<FragmentFile>> register = DaemonHDFS.register;
    protected FormatSelectorI selector;
    protected int id;
	
	public DaemonActivityBasic(CommunicationStream emitterStream, 
							   FormatSelectorI selector,
							   int id) {
		this.emitterStream = emitterStream;
		this.selector = selector;
		this.id = id;
	}

	@Override
	public boolean start(int command, String fileName) throws IOException {
	    boolean result = true;
        if (command == 1) {
        	this.hdfsWrite(fileName);
        } else if (command == 2) {
        	this.hdfsRead(fileName);
        } else if (command == 3) {
        	this.hdfsDelete(fileName);
        } else if (command == 4) {
    		this.hdfsFragmentData(fileName);
    	} else {
	   		result = false;
	   	}
		return result;
	}
    
    private void hdfsWrite(String fileName) throws IOException {
    	// Liste de numéros des fragments du file
    	List<FragmentFile> fragments = register.get(fileName);
		if (fragments == null) {
			fragments = new ArrayList<>();
	        register.put(fileName, fragments);
	    } else {
	    	fragments.clear();
	    }
	    
	    // Dossier des fragments du file
	    String nomDossier = ClusterConfig.PATH + "data/" + fileName + "/";
	    File repertory = new File(nomDossier);
		repertory.mkdir();
		
        // Réception du contenu du fichier
        Integer ordre;
    	while ((ordre = this.emitterStream.receiveDataInt()) >= 0) {
            String file = ClusterConfig.fragmentToName(ordre);
            fragments.add(new FragmentFile(ordre, file));
            
            // Réception du fragment
            String fragment = new String(this.emitterStream.receiveData());
            
            // Récupération de l'éditeur adapté au file
            Format editeur = selector.selectFormat(nomDossier + file);
			editeur.open(OpenMode.W);
            editeur.write(new KV(ordre.toString(), fragment));
			editeur.close();
    	}
    	
    	// Enregisterment de la liste contenant les numéros des fragments dans les noeuds
    	try {
            ObjectOutputStream objectOS = new ObjectOutputStream(new FileOutputStream("register_" + this.id + ".ser"));
            objectOS.writeObject(register);
            objectOS.close();
        } catch(IOException e) {
        	e.printStackTrace();
        }
    }
    
    private void hdfsRead(String fileName) throws IOException {
	    // Dossier des fragments du file
	    String repertoryName = ClusterConfig.PATH + "data/" + fileName + "/";

    	this.hdfsFragmentData(fileName);
    	if (register.containsKey(fileName)) {
    		
			// Réception des numéros des fragments à envoyer
			List<Integer> fragmentsNumbers = new ArrayList<>();
			int fragmentNumber;
			if ((fragmentNumber = this.emitterStream.receiveDataInt()) > 0) {
				for (int i = 0; i < fragmentNumber; i++) {
					fragmentsNumbers.add(this.emitterStream.receiveDataInt());
				}
			}
	    	
			for (Integer number : fragmentsNumbers) {
			    
				// Récupération du lecteur adapté au file
				String file = ClusterConfig.fragmentToName(number);
				Format lecteur = this.selector.selectFormat(repertoryName + file);
				lecteur.open(OpenMode.R);
				KV fragment = lecteur.read();
				byte[] buffer = fragment.k.getBytes();
				this.emitterStream.sendData(buffer, buffer.length);
				buffer = fragment.v.getBytes();
				this.emitterStream.sendData(buffer, buffer.length);
				lecteur.close();
			}
    	}
    }
    
    private void hdfsDelete(String fileName) {
    	File repertory = new File(ClusterConfig.PATH + "data/" + fileName + "/");
	    if (repertory.exists()) {
			for (File file : repertory.listFiles()) {
				file.delete();
			}
		}
    	repertory.delete();
    }
    
    private void hdfsFragmentData(String fileName) throws IOException {
    	// Envoi de la liste des numéros des fragments du fichier
    	List<FragmentFile> fragmentFiles = this.getFragmentData(fileName);
    	if (!fragmentFiles.isEmpty()) {
			this.emitterStream.sendData(fragmentFiles.size());
			for (FragmentFile file : fragmentFiles) {
				this.emitterStream.sendData(file.getNumber());
			}
    	}
    }
    
    private List<FragmentFile> getFragmentData(String fileName) {
    	// Dossier des fragments du file
	    String repertoryName = ClusterConfig.PATH + "data/" + fileName + "/";
	    
	    // Vérification des fragments présents dans le dossier data
	    boolean save = false;
    	List<FragmentFile> fragmentFiles = register.get(fileName);
    	List<String> fragmentFilesNames = new ArrayList<>();
    	List<FragmentFile> modification = new ArrayList<>();
    	File repertory = new File(repertoryName);
		List<String> filesNames = new ArrayList<>();
    	for (File file : repertory.listFiles()) {
    		filesNames.add(file.getName());
		}
    	
    	// Suppression des fragments qui n'ont plus de fichier associé
    	for (FragmentFile file : fragmentFiles) {
    		if (!filesNames.contains(file.getFileName())) {
    			modification.add(file);
    			save = true;
    		} else {
        		fragmentFilesNames.add(file.getFileName());
    		}
    	}
    	fragmentFiles.removeAll(modification);
    	modification.clear();
    	
    	
    	// Addition des fichiers qui n'ont pas de fragment associé
    	for (String file : filesNames) {
    		if (!fragmentFilesNames.contains(file)) {
    			int number = ClusterConfig.nameToFragment(file);
    			fragmentFiles.add(new FragmentFile(number, file));
    			save = true;
    		}
    	}
    	
    	if (fragmentFiles.isEmpty()) {
    		register.remove(fileName);
    	}
    	if (save) {
        	
        	// Enregisterment de la liste contenant les numéros des fragments dans les noeuds
        	try {
                ObjectOutputStream objectOS = new ObjectOutputStream(new FileOutputStream("register_" + this.id + ".ser"));
                objectOS.writeObject(register);
                objectOS.close();
            } catch(IOException e) {
            	e.printStackTrace();
            }
    	}
    	return fragmentFiles;
    }

}
