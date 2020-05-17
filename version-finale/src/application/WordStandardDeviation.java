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

public class WordStandardDeviation implements MapReduce {
	
	private static final long serialVersionUID = 1L;
    private static final String LETTERS = new String("letters");
    private static final String SQUARES = new String("squares");
    private static final String WORDS = new String("words");

	public boolean requiresReader() {
		return true;
	}

	// MapReduce program that computes word-lengths mean
	public void map(FormatReader reader, SynchronizedList<KV> channel, int id) {	
        KV pair;
        Long letters = 0L;
        Long squares = 0L;
        Long words = 0L;
		while ((pair = reader.read()) != null) {
			StringTokenizer separator = new StringTokenizer(pair.v);
			while (separator.hasMoreTokens()) {
				int length = separator.nextToken().length();
                letters += length;
                squares += (long)Math.pow(length, 2);
                words++;
			}
        }
        channel.add(new KV(LETTERS, letters.toString()));
        channel.add(new KV(SQUARES, squares.toString()));
        channel.add(new KV(WORDS, words.toString()));
	}
	
	public void reduce(SynchronizedList<KV> channel, FormatWriter writer) {
		List<KV> input = new ArrayList<>();
		long words = 0;
		long squares = 0;
		long letters = 0;
    	while (channel.waitUntilIsNotEmpty()) {
			channel.removeAllInto(100, input);
    		for (KV pair : input) {
				if (pair.k.equals(LETTERS))
					letters += Long.parseLong(pair.v);
				else if (pair.k.equals(SQUARES))
					squares += Long.parseLong(pair.v);
				else if (pair.k.equals(WORDS))
					words += Long.parseLong(pair.v);
			}
    		input.clear();
		}
        double mean = (((double) letters) / ((double) words));
        mean = Math.pow(mean, 2.0);
        double term = (((double) squares / ((double) words)));
        Double standardDeviation = Math.sqrt((term - mean));
		//System.out.print("Ecart-type des mots : " + standardDeviation);
		writer.write(new KV("Ecart-type des mots", standardDeviation.toString()));
	}
	
	public static void main(String args[]) {
		Job job = new Job();
        job.setInputFormat(Format.Type.LINE);
        job.setInputFile(MapReduce.getFile(args[0]));
		//System.out.println("Execution de l'instance de Job");
        long begin = System.currentTimeMillis();
		job.startJob(new WordStandardDeviation());
		long end = System.currentTimeMillis();
		//System.out.println("Fin de l'éxecution de l'instance de Job");
        //System.out.println("Durée de l'éxecution : " + (end - begin) + "ms");
		System.out.print((end - begin));
        System.exit(0);
	}

}
