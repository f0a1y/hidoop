package hdfs.serveur;

public class ChoixFragmenteurBasique implements ChoixFragmenteurI {

	private int tailleFragment;
	
	public ChoixFragmenteurBasique(int tailleFragment) {
		this.tailleFragment = tailleFragment;
	}
	
	public void setTailleFragment(int tailleFragment) {
		this.tailleFragment = tailleFragment;
	}
	
	@Override
	public Fragmenteur choisirFragmenteur(String nomFichier) {
		if (nomFichier.endsWith(".txt"))
			return new FragmenteurTXT(this.tailleFragment);
		return null;
	}

}
