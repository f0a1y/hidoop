package application;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;
import ordo.SynchronizedList;

public class WordMean implements MapReduce {
	
    private static final long serialVersionUID = 1L;
    private static final String WORDS = new String("words");
    private static final String LETTERS = new String("letters");

	public boolean requiresReader() {
		return true;
	}

	// MapReduce program that computes word-lengths mean
	public void map(FormatReader reader, SynchronizedList<KV> channel, int id) {	
        KV pair;
        Long words = 0L;
        Long letters = 0L;
		while ((pair = reader.read()) != null) {
			StringTokenizer separator = new StringTokenizer(pair.v);
			while (separator.hasMoreTokens()) {
                letters += separator.nextToken().length();
                words++;
			}
        }
        channel.add(new KV(WORDS, letters.toString()));
        channel.add(new KV(LETTERS, words.toString()));
	}
	
	public void reduce(SynchronizedList<KV> channel, FormatWriter writer) {
		List<KV> input = new ArrayList<>();
		long words = 0;
		long letters = 0;
    	while (channel.waitUntilIsNotEmpty()) {
			channel.removeAllInto(100, input);
    		for (KV pair : input) {
				if (pair.k.equals(WORDS))
					words += Long.parseLong(pair.v);
				else if (pair.k.equals(LETTERS))
					letters += Long.parseLong(pair.v);
			}
    		input.clear();
		}
		Double mean = (((double) letters) / ((double) words));
		//System.out.print("Moyenne de lettres par mot : " + mean);
		writer.write(new KV("Moyenne de lettres par mot", mean.toString()));
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
		System.out.println((end - begin));
        System.exit(0);
	}
	
}
