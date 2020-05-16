package application;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;
import ordo.SynchronizedList;

public class MyMapReduce implements MapReduce {
	
    private static final long serialVersionUID = 1L;
    private static final String NBWORDS = new String("words");
    private static final String NBLETTERS = new String("letters");


	// MapReduce program that computes word-lengths mean
	public void map(FormatReader reader, SynchronizedList<KV> channel) {	
		//Map<String, Integer> occurrences = new HashMap<>();
        //Map<Integer, Integer> occurrences = new HashMap<>(); // clé : taille du mot, valeur : nb mots de cette taille
        KV pair;
        long nbwords = 0;
        long nbletters = 0;
		while ((pair = reader.read()) != null) {
			StringTokenizer separator = new StringTokenizer(pair.v);
			while (separator.hasMoreTokens()) {
                /*String word = separator.nextToken();
				if (occurrences.containsKey(length))
					occurrences.put(length, occurrences.get(length) + 1);
				else 
                    occurrences.put(length, 1);*/
                nbletters = nbletter + separator.nextToken().length();
                nbwords =nbword + 1;
			}
        }
        String nbl = Long.toString(nbletters);
        String nbw = Long.toString(nbwords);
		//for (String word : occurrences.keySet())
            //channel.add(new KV(word, occurrences.get(word).toString()));
        channel.add(new KV(NBWORDS, nbl));
        channel.add(new KV(NBLETTERS, nbw));
	}
	
	public void reduce(SynchronizedList<KV> channel, FormatWriter writer) {
        Map<String, Integer> occurrences = new HashMap<>();
    	List<KV> input = new ArrayList<>();
    	while (channel.waitUntilIsNotEmpty()) {
			channel.removeAllInto(100, input);
    		for (KV pair : input) {
				if (occurrences.containsKey(pair.k)) 
					occurrences.put(pair.k, occurrences.get(pair.k) + Integer.parseInt(pair.v));
				else 
					occurrences.put(pair.k, Integer.parseInt(pair.v));
			}
    		input.clear();
		}
		for (String word : occurrences.keySet()) 
			writer.write(new KV(word, occurrences.get(word).toString()));
	}
	
	public static void main(String args[]) {
		Job job = new Job();
        job.setInputFormat(Format.Type.LINE);
        job.setInputFile(MapReduce.getFile(args[0]));
		System.out.println("Execution de l'instance de Job");
        long begin = System.currentTimeMillis();
		job.startJob(new MyMapReduce());
		long end = System.currentTimeMillis();
		System.out.println("Fin de l'�xecution de l'instance de Job");
        System.out.println("Dur�e de l'�xecution : " + (end - begin) + "ms");
        System.exit(0);
	}
	
}
