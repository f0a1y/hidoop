package hdfs.serveur;

public interface ChoixFragmenteurI {

	void setTailleFragment(int tailleFragment);
	
	Fragmenteur choisirFragmenteur(String nomFichier);
	
}
