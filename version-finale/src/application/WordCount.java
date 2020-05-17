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

public class WordCount implements MapReduce {
	
	private static final long serialVersionUID = 1L;

	public boolean requiresReader() {
		return true;
	}
	
	// MapReduce program that computes word counts
	public void map(FormatReader reader, SynchronizedList<KV> channel, int id) {	
		Map<String, Integer> occurrences = new HashMap<>();
		KV pair;
		while ((pair = reader.read()) != null) {
			StringTokenizer separator = new StringTokenizer(pair.v);
			while (separator.hasMoreTokens()) {
				String word = separator.nextToken();
    			Integer occurrence = occurrences.get(word);
				if (occurrence != null)
					occurrences.put(word, occurrence + 1);
				else 
					occurrences.put(word, 1);
			}
		}
		for (Map.Entry<String, Integer> occurrence : occurrences.entrySet()) 
			channel.add(new KV(occurrence.getKey(), occurrence.getValue().toString()));
	}
	
	public void reduce(SynchronizedList<KV> channel, FormatWriter writer) {
        Map<String, Integer> occurrences = new HashMap<>();
    	List<KV> input = new ArrayList<>();
    	while (channel.waitUntilIsNotEmpty()) {
			channel.removeAllInto(100, input);
    		for (KV pair : input) {
    			Integer occurrence = occurrences.get(pair.k);
				if (occurrence != null) 
					occurrences.put(pair.k, occurrence + Integer.parseInt(pair.v));
				else 
					occurrences.put(pair.k, Integer.parseInt(pair.v));
			}
    		input.clear();
		}
		for (Map.Entry<String, Integer> occurrence : occurrences.entrySet()) 
			writer.write(new KV(occurrence.getKey(), occurrence.getValue().toString()));
	}
	
	public static void main(String args[]) {
		Job job = new Job();
        job.setInputFormat(Format.Type.LINE);
        job.setInputFile(MapReduce.getFile(args[0]));
		//System.out.println("Execution de l'instance de Job");
        long begin = System.currentTimeMillis();
		job.startJob(new WordCount());
		long end = System.currentTimeMillis();
		//System.out.println("Fin de l'éxecution de l'instance de Job");
        //System.out.println("Durée de l'éxecution : " + (end - begin) + "ms");
		System.out.print((end - begin));
        System.exit(0);
	}
	
}
