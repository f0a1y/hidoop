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

    // Chemin du projet
    public final static String PATH = "C:\\Users\\Alexandre\\git\\hidoop\\hidoop\\src";

    // Liste des noms d'h�te des noeuds du cluster
    public final static String hosts = {"phenix", "manticore", "cerbere", "pinkfloyd", "medecineman"};

    public final static int ports[][] = {4001, 4002, 4003, 4004, 4005}, {4006, 4007, 4008, 4009, 4010}, {4011, 4012, 4013, 4014, 4015};
    
    // Identifiants des diff�rents services
    public final static int hidoop = 0;
    public final static int hdfs = 1;
    public final static int link = 2;

    // Nombre de noeuds du cluster
    public final static int numberDaemons = 5;
    
    // Nombre max de maps parall�les par daemon
    public final static int numberMaps = 3;
    
    // Nombre de redondance du syst�me de stockage HDFS
    public final static int redundancy = 2;
    
    // Nombre max d'instance KV � envoyer en une seule fois par les daemons
    public final static int numberMaxKV = 100;
    
    // Nombre max d'instance KV pouvant �tre stock�es dans les SynchronizedList
    public final static int numberMaxChannel = 1000;

	// S�lecteur de format de fichier
    public final static FormatSelectorI selector = new FormatSelector();

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
    
    // Choix de la classe impl�mentant l'interface FileDataI � utiliser par le server
    public static FileDataI getFileData(FileDescriptionI file) {
    	return new FileData(file);
    }

    // Choix du registre pour les daemons
    public static FragmentRegisterI getFragmentRegister() {
    	return new FragmentRegister();
    }
    
    // Choix de la classe impl�mentant l'interface FragmentDataI � utiliser par le server
    public static FragmentDataI getFragmentData(FileDescriptionI file, String path) {
    	return new FragmentData(file, path);
    }
    
    // Retourne le chemin du dossier contenant les donn�es du programme
    public static String getDataPath() {
    public final static String PATH = "C:\\Users\\Alexandre\\git\\hidoop\\hidoop\\src";
    }
    
    // Choix du nom du dossier contenant les fragments d'un fichier
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
    	return getDataPath() + fileName.toString();
    }
    
    // Choix du nom sous lequel �crire le fichier
    public static String fileToFileName(FileDescriptionI file) {
		if (file.hasDestinationName())
			return file.getDestinationName();
		else
			return file.getPath() + File.separator + file.getName();
    }
    
    // Choix du nom d'un fichier fragment
    public static String fragmentToName(Integer fragment) {
    	return fragment.toString();
    }
    
}
