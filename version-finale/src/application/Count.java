package application;

/* Note : pour pouvoir fonctionner sans modifications, cette application suppose 
 * l'existence d'un attribut statique PATH d'une classe Projet situ�e dans le r�pertoire 
 * hidoop/src/config. Cet attribut est suppos� contenir le chemin d'acc�s au r�pertoire 
 * hidoop (celui qui contient le r�pertoire applications contenant le pr�sent fichier)
 */
 
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Count {

	public static void main(String[] args) {
		try {
            long begin = System.currentTimeMillis();
			Map<String,Integer> occurrences = new HashMap<>();
			LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(args[0])));
			while (true) {
				String line = reader.readLine();
				if (line == null) 
					break;
				StringTokenizer separator = new StringTokenizer(line);
				while (separator.hasMoreTokens()) {
					String word = separator.nextToken();
					if (occurrences.containsKey(word)) 
						occurrences.put(word, occurrences.get(word) + 1);
					else 
						occurrences.put(word, 1);
				}
			}
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("count-res")));
			for (String word : occurrences.keySet()) {
				writer.write(word + "<->" + occurrences.get(word).toString());
				writer.newLine();
			}
			reader.close();
			writer.close();
            long end = System.currentTimeMillis();
            System.out.println("Dur�e de l'�xecution : " + (end - begin) + "ms");
		} catch (Exception e) {e.printStackTrace();}
	}

}
