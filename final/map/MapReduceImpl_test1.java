package map;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import map.MapReduce;
import ordo.Job;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;


//celui donné par le sujet
public class MapReduceImpl_test1 implements MapReduce {
	private static final long serialVersionUID = 1L;

	// MapReduce program that computes word counts
	public void map(FormatReader reader, FormatWriter writer) {
		
		System.out.println("Je suis dans le map");

		try{
		Thread.sleep(5*1000);
		}catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("J'ai fini le map");
	}
	
	public void reduce(FormatReader reader, FormatWriter writer) {

		System.out.println("Je suis dans le reduce");
		
		try{
		Thread.sleep(5*1000);
		}catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("J'ai fini le reduce");
	}
	
	public static void main(String args[]) {

		System.out.println(" MapReduce1");

		//création d'une instance de Job
		Job j = new Job();

		System.out.println(" MapReduce2");

		// paramétrage du Job
		j.setInputFormat(Format.Type.LINE);
		j.setInputFname(args[0]);

		System.out.println(" MapReduce3");

	   //lancement du MapReduce avec Hidoop
		j.startJob(new MapReduceImpl_test1());

		System.out.println(" MapReduce5");

        System.exit(0);
		}
}