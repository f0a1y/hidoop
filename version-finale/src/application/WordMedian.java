package application;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;
import ordo.SynchronizedList;

public class WordMedian implements MapReduce {
	
	private static final long serialVersionUID = 1L;

	public boolean requiresReader() {
		return true;
	}

	// MapReduce program that computes word-lengths mean
	public void map(FormatReader reader, SynchronizedList<KV> channel, int id) {	
		Map<Integer, Integer> occurrences = new HashMap<>();
		KV pair;
		while ((pair = reader.read()) != null) {
			StringTokenizer separator = new StringTokenizer(pair.v);
			while (separator.hasMoreTokens()) {
				String word = separator.nextToken();
				Integer key = word.length();
    			Integer occurrence = occurrences.get(key);
				if (occurrence != null)
					occurrences.put(key, occurrence + 1);
				else 
					occurrences.put(key, 1);
			}
		}
		for (Map.Entry<Integer, Integer> occurrence : occurrences.entrySet()) 
			channel.add(new KV(occurrence.getKey().toString(), occurrence.getValue().toString()));
	}
	
	public void reduce(SynchronizedList<KV> channel, FormatWriter writer) {
        Map<Integer, Integer> occurrences = new HashMap<>();
    	List<KV> input = new ArrayList<>();
    	while (channel.waitUntilIsNotEmpty()) {
			channel.removeAllInto(100, input);
    		for (KV pair : input) {
    			Integer key = Integer.parseInt(pair.k);
    			Integer occurrence = occurrences.get(key);
				if (occurrence != null) 
					occurrences.put(key, occurrence + Integer.parseInt(pair.v));
				else 
					occurrences.put(key, Integer.parseInt(pair.v));
			}
    		input.clear();
		}
    	Map<Integer, Integer> sorted = occurrences.entrySet()
    											  .stream()
    											  .sorted(Map.Entry.<Integer, Integer>comparingByValue())
    											  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
		for (Map.Entry<Integer, Integer> occurrence : sorted.entrySet()) 
			writer.write(new KV("Mots de taille : " + occurrence.getKey().toString(), occurrence.getValue().toString()));
	}
	
	public static void main(String args[]) {
		Job job = new Job();
        job.setInputFormat(Format.Type.LINE);
        job.setInputFile(MapReduce.getFile(args[0]));
		//System.out.println("Execution de l'instance de Job");
        long begin = System.currentTimeMillis();
		job.startJob(new WordMedian());
		long end = System.currentTimeMillis();
		//System.out.println("Fin de l'éxecution de l'instance de Job");
        //System.out.println("Durée de l'éxecution : " + (end - begin) + "ms");
		System.out.print((end - begin));
        System.exit(0);
	}
	
}
