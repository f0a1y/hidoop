package hdfs.serveur;

import java.util.List;

public abstract class Fragmenteur {
	
	private int tailleFragment;
	
	public Fragmenteur(int tailleFragment) {
		this.tailleFragment = tailleFragment;
	}
	
	public abstract int fragmenter(byte[] trame, int longueur, List<byte[]> fragments);
	
	public int getTailleFragment() {
		return this.tailleFragment;
	}
	
	public void setTailleFragment(int tailleFragment) {
		this.tailleFragment = tailleFragment;
	}

}
