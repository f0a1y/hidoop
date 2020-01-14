package hdfs.serveur;

public class ChoixFragmenteur implements ChoixFragmenteurI {

	private int tailleFragment;
	
	public ChoixFragmenteur(int tailleFragment) {
		this.tailleFragment = tailleFragment;
	}
	
	@Override
	public Fragmenteur choisirFragmenteur(String nomFichier) {
		if (nomFichier.endsWith(".txt"))
			return new FragmenteurTXT(this.tailleFragment);
		return null;
	}

}
