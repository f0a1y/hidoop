import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Configuration {

	public static void main(String[] args) {
		try {
			if (args.length > 1) {
				boolean error = false;
				boolean maps, redundancy, ports;
				maps = redundancy = ports = false;
				int numberArguments = 2;
				int count = 0;
				if (args[count].startsWith("-")) {
					String options = args[count];
					numberArguments += options.length();
					if (options.length() <= 4)
						for (int i = 1; i < numberArguments - 2; i++) {
							switch (options.charAt(i)) {
							case 'm':
								maps = true;
								break;
							case 'r':
								redundancy = true;
								break;
							case 'p':
								ports = true;
								break;
							default:
								error = true;
							}
						}
					else
						error = true;
					count++;
				}
				if (!error && args.length == numberArguments) {
					String path = args[count];
					String javaPath = path.replace("\\", "\\\\");
					count++;
					String[] hosts = args[count].trim().replaceAll("\\s+", " ").split(" ");
					count++;
					int numberPorts, numberMaps, numberRedundancy;
					numberPorts = numberMaps = numberRedundancy = 0;
					if (ports) {
						numberPorts = Integer.parseInt(args[count]);
						count++;
					}
					if (maps) {
						numberMaps = Integer.parseInt(args[count]);
						count++;
					}
					if (redundancy)
						numberRedundancy = Integer.parseInt(args[count]);
					if (!error) {
						Path filePath = Paths.get("config" + File.separator + "ClusterConfig.java");
						List<String> fileContent = new ArrayList<>(Files.readAllLines(filePath, StandardCharsets.ISO_8859_1));
						for (int i = 0; i < fileContent.size(); i++) {
							String line = fileContent.get(i);
						    if (line.contains("public final static String PATH"))
						        fileContent.set(i, "    public final static String PATH = \"" + javaPath + "\";");
						    else if (line.contains("public final static String hosts")) {
						    	StringBuilder newLine = new StringBuilder();
						    	newLine.append("    public final static String[] hosts = {");
						    	for (int j = 0; j < hosts.length - 1; j++)
						    		newLine.append("\"" + hosts[j] + "\", ");
					    		newLine.append("\"" + hosts[hosts.length - 1] + "\"};");
						    	fileContent.set(i, newLine.toString());
						    } else if (line.contains("public final static int numberDaemons"))
						        fileContent.set(i, "    public final static int numberDaemons = " + hosts.length + ";");
						    else if (ports && line.contains("public final static int[][] ports")) {
						    	StringBuilder newLine = new StringBuilder();
						    	newLine.append("    public final static int ports[][] = {{");
						    	int currentPort = numberPorts;
						    	for (int j = 0; j < 2; j++) {
						    		for (int k = 0; k < hosts.length - 1; k++)
						    			newLine.append(currentPort++ + ", ");
						    		newLine.append("" + currentPort++ + "}, {");
						    	}
					    		for (int k = 0; k < hosts.length - 1; k++)
					    			newLine.append(currentPort++ + ", ");
					    		newLine.append("" + currentPort++ + "}};");
						    	fileContent.set(i, newLine.toString());
						    } 
						    else if (maps && line.contains("public final static int numberMaps")) {
						        fileContent.set(i, "    public final static int numberMaps = " + numberMaps + ";");
						    }
						    else if (redundancy && line.contains("public final static int redundancy")) {
						        fileContent.set(i, "    public final static int redundancy = " + numberRedundancy + ";");
						    }
						}
						Files.write(filePath, fileContent, StandardCharsets.ISO_8859_1);

						/*filePath = Paths.get("lancer-hidoop.sh");
						fileContent = new ArrayList<>();
						fileContent.add("javac -encoding ISO-8859-1 ");
						fileContent.add("mate-terminal --window -e \"/bin/bash -c \\\"java hdfs.server.ServerHDFS; exec /bin/bash\\\"\" \\");
						for (int i = 0; i < hosts.length - 1; i++)
							fileContent.add("--tab -e \"/bin/bash -c \\\"ssh $USER@" + hosts[i] + " 'cd " + path + " && java hdfs.daemon.DaemonHDFS " + i + "'; exec /bin/bash\\\"\" \\");
						fileContent.add("--tab -e \"/bin/bash -c \\\"ssh $USER@" + hosts[hosts.length - 1] + " 'cd " + path + " && java hdfs.daemon.DaemonHDFS " + (hosts.length - 1) + "'; exec /bin/bash\\\"\"");
						
						fileContent.add("mate-terminal --window -e \"/bin/bash -c \\\"ssh $USER@" + hosts[0] + " 'cd " + path + " && java ordo.DaemonImpl " + 0 + "'; exec /bin/bash\\\"\" \\");
						for (int i = 1; i < hosts.length - 1; i++)
							fileContent.add("--tab -e \"/bin/bash -c \\\"ssh $USER@" + hosts[i] + " 'cd " + path + " && java ordo.DaemonImpl " + i + "'; exec /bin/bash\\\"\" \\");
						fileContent.add("--tab -e \"/bin/bash -c \\\"ssh $USER@" + hosts[hosts.length - 1] + " 'cd " + path + " && java ordo.DaemonImpl " + (hosts.length - 1) + "'; exec /bin/bash\\\"\"");
						Files.write(filePath, fileContent, StandardCharsets.ISO_8859_1);*/
						filePath = Paths.get("lancer-hidoop.sh");
						fileContent = new ArrayList<>();
						fileContent.add("javac -encoding ISO-8859-1 */*.java */*/*.java");
						fileContent.add("mate-terminal --window -e \"/bin/bash -c \\\"java hdfs.server.ServerHDFS; exec /bin/bash\\\"\" \\");
						for (int i = 0; i < hosts.length - 1; i++) {
							fileContent.add("ssh $USER@" + hosts[hosts.length - 1] + " 'cd " + path + " && java hdfs.daemon.DaemonHDFS " + (hosts.length - 1) + "' &");
							fileContent.add("ssh $USER@" + hosts[hosts.length - 1] + " 'cd " + path + " && java ordo.DaemonImpl " + (hosts.length - 1) + "' &");
						}

						filePath = Paths.get("clean.sh");
						fileContent = new ArrayList<>();
						for (int i = 0; i < hosts.length; i++)
							fileContent.add("ssh $USER@" + hosts[i] + " 'pkill -f java.*DaemonHDFS*'");

						for (int i = 0; i < hosts.length; i++)
							fileContent.add("ssh $USER@" + hosts[i] + " 'pkill -f java.*DaemonImpl*'");
						fileContent.add("pkill -f java.*ServerHDFS*");
						Files.write(filePath, fileContent, StandardCharsets.ISO_8859_1);

						filePath = Paths.get("preparation-ssh.sh");
						fileContent = new ArrayList<>();
						fileContent.add("ssh-keygen -t rsa");
						for (int i = 0; i < hosts.length; i++)
							fileContent.add("ssh-copy-id $USER@" + hosts[i]);
						Files.write(filePath, fileContent, StandardCharsets.ISO_8859_1);
					}
				}
			}
		} catch (Exception e) {e.printStackTrace();}
	}
	
}
	
