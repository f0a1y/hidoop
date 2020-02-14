package config;

import formats.FormatSelectorBasic;
import formats.FormatSelectorI;
import hdfs.ActivityI;
import hdfs.CommunicationStream;
import hdfs.daemon.DaemonActivityBasic;
import hdfs.server.ServerActivityBasic;

public class ClusterConfig {

    // Liste des noms d'hôte des noeuds du cluster
	// pour Hidoop fonctionnant seulement sur une machine
	public final static String nomMachine[] = {"localhost", "localhost", "localhost", "localhost", "localhost", "localhost"};

    // Liste des noms d'hôte des noeuds du cluster
    // pour Hidoop fonctionnant sur plusieurs machines
    // public final static String nomMachine[] = {"Griffon", "Pixie", "manticore", "nymphe", "succube", "cerbere"};

    // Liste des ports des daemons HDFS et Hidoop des noeuds du cluster
    public final static int numPortHidoop[] = {4500, 4501, 4502, 4503, 4504, 4505};
    public final static int numPortHDFS[] = {4100, 4101, 4102, 4103, 4104, 4105};

    // Nombre de noeuds du cluster
    public final static int nbMachine = 2;

	// Sélecteur de format de fichier
    public final static FormatSelectorI selector = new FormatSelectorBasic();
    
    // Nombre de redondance du système de stockage HDFS
    public final static int redundancy = 1;

    // Choix du comportement du serveur HDFS
    public static ActivityI getServerActivity(CommunicationStream clientStream) {
    	return new ServerActivityBasic(clientStream);
    }

    // Choix du comportement des daemons HDFS 
    public static ActivityI getDaemonActivity(CommunicationStream emitterStream, 
    										  int id) {
    	return new DaemonActivityBasic(emitterStream, selector, id);
    }
    
    public static String fragmentToName(Integer fragment) {
    	return fragment.toString();
    }
    
    public static int nameToFragment(String name) {
    	return Integer.parseInt(name);
    }

    // Chemin du projet
    public final static String PATH = "/home/achatain/Documents/hidoopBis/final/";
    
}
