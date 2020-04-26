package config;

import java.io.File;
import java.net.Socket;

import formats.FormatSelector;
import formats.FormatSelectorI;
import hdfs.ActivityI;
import hdfs.FileDescriptionI;
import hdfs.daemon.DaemonActivity;
import hdfs.daemon.FragmentData;
import hdfs.daemon.FragmentDataI;
import hdfs.daemon.FragmentRegister;
import hdfs.daemon.FragmentRegisterI;
import hdfs.server.FileData;
import hdfs.server.FileDataI;
import hdfs.server.FileRegister;
import hdfs.server.FileRegisterI;
import hdfs.server.ServerActivity;

public class ClusterConfig {

    // Liste des noms d'h�te des noeuds du cluster
	// pour Hidoop fonctionnant seulement sur une machine
	public final static String hosts[] = {"localhost", "localhost", "localhost", "localhost", "localhost", "localhost"};

    // Liste des noms d'hôte des noeuds du cluster
    // pour Hidoop fonctionnant sur plusieurs machines
    // public final static String nomMachine[] = {"Griffon", "Pixie", "manticore", "nymphe", "succube", "cerbere"};

    // Liste des ports des daemons HDFS et Hidoop des noeuds du cluster
    public final static int hidoopPorts[] = {4500, 4501, 4502, 4503, 4504, 4505};
    public final static int hdfsPorts[] = {4100, 4101, 4102, 4103, 4104, 4105};

    // Nombre de noeuds du cluster
    public final static int numberDaemons = 3;
    
    public final static int numberMaps = 3;

	// Sélecteur de format de fichier
    public final static FormatSelectorI selector = new FormatSelector();
    
    // Nombre de redondance du système de stockage HDFS
    public final static int redundancy = 2;

    // Choix du comportement du serveur HDFS
    public static ActivityI getServerActivity(Socket client) {
    	return new ServerActivity(client);
    }

    // Choix du comportement des daemons HDFS 
    public static ActivityI getDaemonActivity(Socket emitter, int id) {
    	return new DaemonActivity(emitter, selector, id);
    }

    // Choix du registre pour le serveur
    public static FileRegisterI getFileRegister() {
    	return new FileRegister();
    }
    
    public static FileDataI getFileData(FileDescriptionI file) {
    	return new FileData(file);
    }

    // Choix du registre pour les daemons
    public static FragmentRegisterI getFragmentRegister() {
    	return new FragmentRegister();
    }
    
    public static FragmentDataI getFragmentData(FileDescriptionI file, String path) {
    	return new FragmentData(file, path);
    }
    
    public static String getDataPath() {
    	return PATH + "data" + File.separator;
    }
    
    public static String fileToRepertory(FileDescriptionI file, int id) {
    	StringBuilder fileName = new StringBuilder();
    	fileName.append(file.getPath().replace(':', '-').replace('\\', '-').replace('/', '-'));
    	fileName.append('_');
    	fileName.append(file.getName().replace(':', '-').replace('\\', '-').replace('/', '-'));
    	if (file.hasAlias()) {
        	fileName.append('_');
        	fileName.append(file.getAlias());
    	}
    	fileName.append('_');
    	fileName.append(id);
    	fileName.append(File.separator);
    	return fileName.toString();
    }
    
    public static String fileToFileName(FileDescriptionI file) {
		if (file.hasDestinationName())
			return file.getDestinationName();
		else
			return file.getPath() + File.separator + file.getName();
    }
    
    public static String fragmentToName(Integer fragment) {
    	return fragment.toString();
    }
    
    public static int nameToFragment(String name) {
    	return Integer.parseInt(name);
    }

    // Chemin du projet
    public final static String PATH = "C:\\Users\\Alexandre\\git\\hidoop\\H2_AV1_Jezequel_Lacaille_Roth_Chatain_Benabbi\\src\\";
    
}
