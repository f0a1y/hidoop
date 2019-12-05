package hdfs;

import java.util.ArrayList;
import java.util.List;

public class FragmentTexte {

	private int ordre;
	private String texte;
	private List<String> fragments;

	public FragmentTexte(int ordre, String texte) {
		this.ordre = ordre;
		this.texte = texte;
		this.fragments = new ArrayList<>();
	}
	
	public int getOrdre() {
		return ordre;
	}

	public String getTexte() {
		return texte;
	}	
	
}
