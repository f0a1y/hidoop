package hdfs.serveur;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class FragmenteurTXT extends Fragmenteur {
	
	public FragmenteurTXT(int tailleFragment) {
		super(tailleFragment);
	}
	
	@Override
	public int fragmenter(byte[] trame, int longueur, List<byte[]> fragments) {
		int indice = 0;		
		String reste = null;
		StringBuilder fragment = new StringBuilder();
		while (indice + this.getTailleFragment() <= longueur) {			
			fragment.append(new String(trame, indice, this.getTailleFragment()));
			int separation = fragment.lastIndexOf(" ");
			fragments.add(fragment.substring(0, separation).getBytes());
			indice += this.getTailleFragment();
			reste = fragment.substring(separation);
			fragment.setLength(0);
			fragment.append(reste);
		}
		if (indice > 0) {
			return indice - reste.getBytes().length;
		}
		return 0;
	}

}
