package application;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import config.ClusterConfig;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;
import ordo.SynchronizedList;

public class MonteCarlo implements MapReduce {

	private static int number = 0;
	private static ReentrantLock gate = new ReentrantLock();
	private long size;
	
	public MonteCarlo(long size) {
		this.size = size;
	}
	
	private int getNextNumber() {
		gate.lock();
		int result = number;
		number++;
		gate.unlock();
		return result;
	}
	
	private static class HaltonSequence {
	    
		/** Bases */
		static final int[] P = {2, 3}; 
	    
		/** Maximum number of digits allowed */
		static final int[] K = {63, 40}; 

		private long index;
		private double[] x;
		private double[][] q;
		private int[][] d;

		/** Initialize to H(startindex),
		 * so the sequence begins with H(startindex+1).
		 */
		HaltonSequence(long startindex) {
			this.index = startindex;
			this.x = new double[K.length];
			this.q = new double[K.length][];
			this.d = new int[K.length][];
			for(int i = 0; i < K.length; i++) {
				this.q[i] = new double[K[i]];
				this.d[i] = new int[K[i]];
			}
			for(int i = 0; i < K.length; i++) {
				long k = index;
				this.x[i] = 0;    
				for(int j = 0; j < K[i]; j++) {
					this.q[i][j] = (j == 0 ? 1.0 : this.q[i][j-1])/P[i];
					this.d[i][j] = (int)(k % P[i]);
					k = (k - this.d[i][j])/P[i];
					this.x[i] += this.d[i][j] * this.q[i][j];
				}
			}
		}

		/** Compute next point.
		 * Assume the current point is H(index).
		 * Compute H(index+1).
		 * 
		 * @return a 2-dimensional point with coordinates in [0,1)^2
		 */
		double[] nextPoint() {
			this.index++;
			for(int i = 0; i < K.length; i++) {
				for(int j = 0; j < K[i]; j++) {
					this.d[i][j]++;
					this.x[i] += this.q[i][j];
					if (this.d[i][j] < P[i]) {
						break;
					}
					this.d[i][j] = 0;
					this.x[i] -= (j == 0 ? 1.0 : this.q[i][j-1]);
				}
			}
			return x;
		}
	
	}
	
	private static final long serialVersionUID = 1L;

	public boolean requiresReader() {
		return false;
	}
	
	// MapReduce program that computes word counts
	public void map(FormatReader reader, SynchronizedList<KV> channel, int id) {	
		final HaltonSequence haltonsequence = new HaltonSequence(((id * ClusterConfig.numberMaps) + getNextNumber()) * this.size);
		Long numInside = 0L;
		Long numOutside = 0L;

		for(long i = 0; i < this.size; i++) {
			//generate points in a unit square
			final double[] point = haltonsequence.nextPoint();

			//count points inside/outside of the inscribed circle of the square
			final double x = point[0] - 0.5;
			final double y = point[1] - 0.5;
			if (x*x + y*y > 0.25) {
				numOutside++;
			} else {
				numInside++;
			}
		}

		//output map results
		channel.add(new KV("Inside", numInside.toString()));
		channel.add(new KV("Outside", numOutside.toString()));
	}
	
	public void reduce(SynchronizedList<KV> channel, FormatWriter writer) {
        Long numInside = 0L;
        Long numOutside = 0L;
    	List<KV> input = new ArrayList<>();
    	while (channel.waitUntilIsNotEmpty()) {
			channel.removeAllInto(100, input);
    		for (KV pair : input) {
				if (pair.k.equals("Inside")) 
					numInside += Long.parseLong(pair.v);
				else 
					numOutside += Long.parseLong(pair.v);
			}
    		input.clear();
    	}
    	BigDecimal numTotal = BigDecimal.valueOf(ClusterConfig.numberDaemons * ClusterConfig.numberMaps * this.size);
    	BigDecimal result = BigDecimal.valueOf(4).setScale(20)
    					 			  .multiply(BigDecimal.valueOf(numInside))
    					 			  .divide(numTotal, RoundingMode.HALF_UP);
    	
		System.out.print(result);
		writer.write(new KV("Valeur estimée de Pi", result.toString()));
	}
	
	public static void main(String args[]) {
		Job job = new Job();
        job.setInputFormat(Format.Type.LINE);
		//System.out.println("Execution de l'instance de Job");
        long begin = System.currentTimeMillis();
		job.startJob(new MonteCarlo(Long.parseLong(args[0])));
		long end = System.currentTimeMillis();
		//System.out.println("Fin de l'éxecution de l'instance de Job");
        //System.out.println("Durée de l'éxecution : " + (end - begin) + "ms");
        System.exit(0);
	}
	
}
