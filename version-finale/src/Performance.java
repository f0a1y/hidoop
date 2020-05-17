import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class Performance {

	private static int[] daemons = {1, 2, 3, 5, 10, 15, 20};
	private static int[] maps = {1, 2, 4, 8, 16};
	private static int[] lengths = {1000, 100000, 1000000, 10000000};
	private static String[] files = {"test1Ko.txt", "test10Ko.txt", "test100Ko.txt", "test1Mo.txt", "test10Mo.txt", "test100Mo.txt", "test1Go.txt", "test10Go.txt"};
	private static int[] samples = {20, 20, 15, 10, 8, 4, 2, 1, 1}; 
	private static long[] points = {1000, 1000000, 10000000, 100000000, 500000000, 1000000000, 10000000000L};
	private static long[] samplesPoints = {100, 80, 60, 40, 20, 10, 5, 2};
	private static String[] hosts = {"griffon", "manticore", "pinkfloyd", "acdc", "angel", "lovelace", "rouget", "truite", "magma", "boole", "lannister", "newton", "gandalf", "pikachu", "lafontaine", "uranium", "diabolo", "vador", "titane", "scorpion"};
	private static int[][] configurations = {{0, 0, 3}, {3, 2, 4}, {4, 3, 5}};
	
	public static void main(String[] args) {
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("performance.sh")));
			writer.write("printf \"Count [Maps : 1, Fichier : 1Mo];Mean [Maps : 1, Fichier : 1Mo];Median [Maps : 1, Fichier : 1Mo];$Standard Deviation [Maps : 1, Fichier : 1Mo];\" >> performances-daemon.csv");
			writer.newLine();
			writer.write("printf \"Count [Maps : 4, Fichier : 10Mo];Mean [Maps : 4, Fichier : 10Mo];Median [Maps : 4, Fichier : 10Mo];$Standard Deviation [Maps : 4, Fichier : 10Mo];\" >> performances-daemon.csv");
			writer.newLine();
			writer.write("printf \"Count [Maps : 8, Fichier : 100Mo];Mean [Maps : 8, Fichier : 100Mo];Median [Maps : 8, Fichier : 100Mo];$Standard Deviation [Maps : 8, Fichier : 100Mo];\\n\" >> performances-daemon.csv");
			writer.newLine();
			for (int i = 0; i < daemons.length; i++) {
				for (int j = 0; j < configurations.length; j++) {
					StringBuilder command = new StringBuilder();
					command.append("java Configuration -mf /home/achatain/nosave/src/ \"");
					for (int k = 0; k < daemons[i]; k++)
						command.append(hosts[k] + " ");
					command.append("\" " + maps[configurations[j][1]] + " " + lengths[2]);
					writer.write(command.toString());
					writer.newLine();
					writer.write("sh lancer-hidoop.sh");
					writer.newLine();
					writeCommands(writer, configurations[j][2]);
					writer.write("printf \"%i;%i;%i;%i;\" $countTime $meanTime $medianTime $standardDeviationTime >> performances-daemon.csv");
					writer.newLine();
					writer.write("rm -r data/*");
					writer.newLine();
					writer.write("sh clean.sh");
					writer.newLine();
				}
				writer.write("printf \"\\n\" >> performances-daemon.csv");
				writer.newLine();
			}
			writer.write("printf \"Count [Daemons : 1, Fichier : 1Mo];Mean [Daemons : 1, Fichier : 1Mo];Median [Daemons : 1, Fichier : 1Mo];$Standard Deviation [Daemons : 1, Fichier : 1Mo];\" >> performances-map.csv");
			writer.newLine();
			writer.write("printf \"Count [Daemons : 5, Fichier : 10Mo];Mean [Daemons : 5, Fichier : 10Mo];Median [Daemons : 5, Fichier : 10Mo];$Standard Deviation [Daemons : 5, Fichier : 10Mo];\" >> performances-map.csv");
			writer.newLine();
			writer.write("printf \"Count [Daemons : 10, Fichier : 100Mo];Mean [Daemons : 10, Fichier : 100Mo];Median [Daemons : 10, Fichier : 100Mo];$Standard Deviation [Daemons : 10, Fichier : 100Mo];\\n\" >> performances-map.csv");
			writer.newLine();
			for (int i = 0; i < maps.length; i++) {
				for (int j = 0; j < configurations.length; j++) {
					StringBuilder command = new StringBuilder();
					command.append("java Configuration -mf /home/achatain/nosave/src/ \"");
					for (int k = 0; k < daemons[configurations[j][0]]; k++)
						command.append(hosts[k] + " ");
					command.append("\" " + maps[i] + " " + lengths[2]);
					writer.write(command.toString());
					writer.newLine();
					writer.write("sh lancer-hidoop.sh");
					writer.newLine();
					writeCommands(writer, configurations[j][2]);
					writer.write("printf \\\"%i;%i;%i;%i;\\\" $countTime $meanTime $medianTime $standardDeviationTime >> performances-map.csv");
					writer.newLine();
					writer.write("rm -r data/*");
					writer.newLine();
					writer.write("sh clean.sh");
					writer.newLine();
				}
				writer.write("printf \"\\n\" >> performances-map.csv");
				writer.newLine();
			}
			writer.write("printf \"Count [Daemons : 1, Map : 1, Fichier : 1Mo];Mean [Daemons : 1, Map : 1, Fichier : 1Mo];Median [Daemons : 1, Map : 1, Fichier : 1Mo];$Standard Deviation [Daemons : 1, Map : 1, Fichier : 1Mo];\" >> performances-fragment.csv");
			writer.newLine();
			writer.write("printf \"Count [Daemons : 5, Map : 4, Fichier : 10Mo];Mean [Daemons : 5, Map : 4, Fichier : 10Mo];Median [Daemons : 5, Map : 4, Fichier : 10Mo];$Standard Deviation [Daemons : 5, Map : 4, Fichier : 10Mo];\" >> performances-fragment.csv");
			writer.newLine();
			writer.write("printf \"Count [Daemons : 10, Map : 8, Fichier : 100Mo];Mean [Daemons : 10, Map : 8, Fichier : 100Mo];Median [Daemons : 10, Map : 8, Fichier : 100Mo];$Standard Deviation [Daemons : 10, Map : 8, Fichier : 100Mo];\\n\" >> performances-fragment.csv");
			writer.newLine();
			for (int i = 0; i < lengths.length; i++) {
				for (int j = 0; j < configurations.length; j++) {
					StringBuilder command = new StringBuilder();
					command.append("java Configuration -mf /home/achatain/nosave/src/ \"");
					for (int k = 0; k < daemons[configurations[j][0]]; k++)
						command.append(hosts[k] + " ");
					command.append("\" " + maps[configurations[j][1]] + " " + lengths[i]);
					writer.write(command.toString());
					writer.newLine();
					writer.write("sh lancer-hidoop.sh");
					writer.newLine();
					writeCommands(writer, configurations[j][2]);
					writer.write("printf \\\"%i;%i;%i;%i;\\\" $countTime $meanTime $medianTime $standardDeviationTime >> performances-fragment.csv");
					writer.newLine();
					writer.write("rm -r data/*");
					writer.newLine();
					writer.write("sh clean.sh");
					writer.newLine();
				}
				writer.write("printf \"\\n\" >> performances-fragment.csv");
				writer.newLine();
			}
			writer.write("printf \"Count [Daemons : 1, Map : 1];Mean [Daemons : 1, Map : 1];Median [Daemons : 1, Map : 1];$Standard Deviation [Daemons : 1, Map : 1];\" >> performances-file.csv");
			writer.newLine();
			writer.write("printf \"Count [Daemons : 5, Map : 4];Mean [Daemons : 5, Map : 4];Median [Daemons : 5, Map : 4];$Standard Deviation [Daemons : 5, Map : 4];\" >> performances-file.csv");
			writer.newLine();
			writer.write("printf \"Count [Daemons : 10, Map : 8];Mean [Daemons : 10, Map : 8];Median [Daemons : 10, Map : 8];$Standard Deviation [Daemons : 10, Map : 8];\\n\" >> performances-file.csv");
			writer.newLine();
			for (int i = 0; i < files.length - 2; i++) {
				for (int j = 0; j < configurations.length; j++) {
					StringBuilder command = new StringBuilder();
					command.append("java Configuration -mf /home/achatain/nosave/src/ \"");
					for (int k = 0; k < daemons[configurations[j][0]]; k++)
						command.append(hosts[k] + " ");
					command.append("\" " + maps[configurations[j][1]] + " " + lengths[2]);
					writer.write(command.toString());
					writer.newLine();
					writer.write("sh lancer-hidoop.sh");
					writer.newLine();
					writeCommands(writer, i);
					writer.write("printf \\\"%i;%i;%i;%i;\\\" $countTime $meanTime $medianTime $standardDeviationTime >> performances-file.csv");
					writer.newLine();
					writer.write("rm -r data/*");
					writer.newLine();
					writer.write("sh clean.sh");
					writer.newLine();
				}
				writer.write("printf \"\\n\" >> performances-file.csv");
				writer.newLine();
			}
			writer.write("printf \"[Daemons : 1, Map : 1];\" >> performances-point.csv");
			writer.newLine();
			writer.write("printf \"[Daemons : 5, Map : 4];\" >> performances-point.csv");
			writer.newLine();
			writer.write("printf \"[Daemons : 10, Map : 8];\\n\" >> performances-point.csv");
			writer.newLine();
			for (int i = 0; i < points.length; i++) {
				for (int j = 0; j < configurations.length; j++) {
					StringBuilder command = new StringBuilder();
					command.append("java Configuration -m /home/achatain/nosave/src/ \"");
					for (int k = 0; k < daemons[configurations[j][0]]; k++)
						command.append(hosts[k] + " ");
					command.append("\" " + maps[configurations[j][1]]);
					writer.write(command.toString());
					writer.newLine();
					writer.write("sh lancer-hidoop.sh");
					writer.newLine();
					for (int k = 0; k < samplesPoints[i]; k++) {
						writer.write("printf $(java application.MonteCarlo " + points[i] +") >> performances-point" + j + ".csv");
						writer.newLine();	
						writer.write("printf \"\\n\" >> performances-point" + j + ".csv");
						writer.newLine();	
					}
					writer.write("sh clean.sh");
					writer.newLine();
				}
			}
			writer.close();
		} catch (Exception e) {e.printStackTrace();}
	}
			
	private static void writeCommands(BufferedWriter writer, int file) throws IOException {
		writer.write("java hdfs.ClientHDFS \"1, " + files[file] + "\"");
		writer.newLine();
		writer.write("countTime=0");
		writer.newLine();
		writer.write("meanTime=0");
		writer.newLine();
		writer.write("medianTime=0");
		writer.newLine();
		writer.write("standardDeviationTime=0");
		writer.newLine();
		for (int i = 0; i < samples[file]; i++) {
			writer.write("countTime=$(($countTime+$(java application.WordCount " + files[file] + ")))");
			writer.newLine();
			writer.write("meanTime=$(($meanTime+$(java application.WordMean " + files[file] + ")))");
			writer.newLine();
			writer.write("medianTime=$(($medianTime+$(java application.WordMedian " + files[file] + ")))");
			writer.newLine();
			writer.write("standardDeviationTime=$(($standardDeviationTime+$(java application.WordStandardDeviation " + files[file] + ")))");
			writer.newLine();
		}
		writer.write("countTime=$(($countTime/" + samples[file] + ")");
		writer.newLine();
		writer.write("meanTime=$(($meanTime/" + samples[file] + ")");
		writer.newLine();
		writer.write("medianTime=$(($medianTime/" + samples[file] + ")");
		writer.newLine();
		writer.write("standardDeviationTime=$(($standardDeviationTime/" + samples[file] + ")");
		writer.newLine();
	}
	
}
