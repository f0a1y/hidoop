package hdfs.serveur;

public class LanceurServeur {

    public static void main(String[] args) {
    	ChoixFragmenteur choix = new ChoixFragmenteur(1024);
    	ServerHDFS.lancer(args, choix);
    }
	
}
