package map;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;
import ordo.SynchronizedList;

//celui donné par le sujet
public class MapReduceImpl implements MapReduce {
	
	private static final long serialVersionUID = 1L;

	// MapReduce program that computes word counts
	public void map(FormatReader reader, SynchronizedList<KV> channel) {
		System.out.println("Je suis dans le map");
		try{
			Thread.sleep(5*1000);
		} catch (Exception e) {e.printStackTrace();}
		System.out.println("J'ai fini le map");
	}
	
	public void reduce(SynchronizedList<KV> channel, FormatWriter writer) {
		System.out.println("Je suis dans le reduce");
		try{
			Thread.sleep(5*1000);
		} catch (Exception e) {e.printStackTrace();}
		System.out.println("J'ai fini le reduce");
	}
	
	public static void main(String args[]) {
		Job job = new Job();
		job.setInputFormat(Format.Type.LINE);
		job.setInputFile(MapReduce.getFile(args[0]));
		System.out.println("Execution de l'instance de Job");
		long begin = System.currentTimeMillis();
		job.startJob(new MapReduceImpl());
		long end = System.currentTimeMillis();
		System.out.println("Fin de l'éxecution de l'instance de Job");
        System.out.println("Durée de l'éxecution : " + (end - begin) + "ms");
        System.exit(0);
	}

}