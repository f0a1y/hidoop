package application;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class MyMapReduce implements MapReduce {
	
	private static final long serialVersionUID = 1L;

	// MapReduce program that computes word counts
	public void map(FormatReader reader, FormatWriter writer) {	
		System.out.println("Je suis dans le map");
		Map<String, Integer> occurrences = new HashMap<>();
		KV pair;
		while ((pair = reader.read()) != null) {
			StringTokenizer separator = new StringTokenizer(pair.v);
			while (separator.hasMoreTokens()) {
				String word = separator.nextToken();
				if (occurrences.containsKey(word))
					occurrences.put(word, occurrences.get(word) + 1);
				else 
					occurrences.put(word, 1);
			}
		}
		for (String word : occurrences.keySet()) 
			writer.write(new KV(word, occurrences.get(word).toString()));
		System.out.println("J'ai fini le map");
	}
	
	public void reduce(FormatReader reader, FormatWriter writer) {
		System.out.println("Je suis dans le reduce");
        Map<String, Integer> occurrences = new HashMap<>();
		KV pair;
		while ((pair = reader.read()) != null) {
			if (occurrences.containsKey(pair.k)) 
				occurrences.put(pair.k, occurrences.get(pair.k) + Integer.parseInt(pair.v));
			else 
				occurrences.put(pair.k, Integer.parseInt(pair.v));
		}
		for (String word : occurrences.keySet()) 
			writer.write(new KV(word, occurrences.get(word).toString()));
		System.out.println("J'ai fini le reduce");
	}
	
	public static void main(String args[]) {
		Job job = new Job();
        job.setInputFormat(Format.Type.LINE);
        job.setInputFile(MapReduce.getFile(args[0]));
		System.out.println("Execution de l'instance de Job");
        long begin = System.currentTimeMillis();
		job.startJob(new MyMapReduce());
		long end = System.currentTimeMillis();
		System.out.println("Fin de l'éxecution de l'instance de Job");
        System.out.println("Durée de l'éxecution : " + (end - begin) + "ms");
        System.exit(0);
	}
	
}
