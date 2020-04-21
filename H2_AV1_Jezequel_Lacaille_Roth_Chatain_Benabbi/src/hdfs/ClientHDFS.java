package hdfs;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import config.ClientConfig;
import config.GeneralConfig;

public class ClientHDFS {

    public static void main(String[] args) {
    	if (args.length == 1) {
			boolean groupError, commandError, fileError;
    		String[] groups = args[0].replaceAll("\\s+", " ").split(";");
    		List<List<Command>> commandList = new ArrayList<>();
			List<List<FileDescriptionI>> fileList = new ArrayList<>();
			HashMap<Integer, String> errors = new HashMap<>();
    		for (int i = 0; i < groups.length; i++) {
    			groupError = commandError = fileError = false;
    			String[] parameters = groups[i].split(",");
    			if (parameters.length <= 2) {

    				// Lecture des commandes
    				commandError = getCommands(parameters[0], commandList);

    				//Lecture des descriptions des fichiers
	    			List<FileDescriptionI> fileGroupList = new ArrayList<>();
	    			if (parameters.length == 2)
	    				fileError = getFiles(parameters[1], fileGroupList);
	    			fileList.add(fileGroupList);
	    			
    			} else 
    				groupError = true;
    			if (groupError || commandError || fileError)
    				errors.put(i, groups[i]);
    		}
    		
    		// Suppression des groupes d'op�rations incorrect
    		int i = 0;
    		Iterator<List<Command>> commandGroupIterator = commandList.iterator();
    		Iterator<List<FileDescriptionI>> fileGroupIterator = fileList.iterator();
        	while (commandGroupIterator.hasNext() && fileGroupIterator.hasNext()) {
        		if (errors.containsKey(i)) {
        			commandGroupIterator.next();
        			commandGroupIterator.remove();
        			fileGroupIterator.next();
        			fileGroupIterator.remove();
        		}
        		else if (verifyGroups(commandGroupIterator, fileGroupIterator))
    				errors.put(i, groups[i]);
        		i++;
        	}
    		
        	if (errors.size() > 0) {
        		for (Integer index : errors.keySet())
        			printGroupError(index, errors.get(index));
        		printCommandUse();
        		printFileUse();
        	}
        	else if (commandList.size() > 0 && fileList.size() > 0) {
			    try {
			    	ActivityI activity = ClientConfig.getClientActivity(commandList, fileList);
			    	activity.start();
			    	activity.terminate();
		        } catch (Exception e) {e.printStackTrace();}
    		}
    	} else {
    		printUse();
    		printCommandUse();
    		printFileUse();
    	}
    }
    
    private static boolean getCommands(String parameter, List<List<Command>> commandList) {
		boolean error = false;
		List<Command> commandGroupList = new ArrayList<>();
		parameter = parameter.trim();
		String[] commandGroup = parameter.split(" ");
		for (String commandLabel : commandGroup) {
			try {
				Command command = Command.intToCommand(Integer.parseInt(commandLabel));
				if (command != null)
					error = true;
				else 
					commandGroupList.add(command);
					
			} catch (Exception e) {error = true;}
		}
		commandList.add(commandGroupList);
		return error;
    }
    
    private static boolean getFiles(String parameter, List<FileDescriptionI> fileGroupList) {
    	boolean error = false;
    	String[] fileGroup = parameter.trim()
    								  .replaceAll(" -a ", "\0\1")
    								  .replaceAll(" -d ", "\0\2")	
    								  .split(" ");
		for (String fileLabel : fileGroup) {
			String fileName, filePath, fileAlias, fileNameDestination;
			fileAlias = fileNameDestination = null;
			if (!fileLabel.startsWith("\0")) {
				String[] splits = fileLabel.split("\0");
				fileName = splits[0];
				File file = new File(fileName);
				if (file.exists()) {
					fileName = file.getName();
					filePath = file.getAbsolutePath();
				} else 
					filePath = Paths.get(".").toAbsolutePath().normalize().toString();
				for (int j = 1; j < splits.length; j++) {
					switch (splits[j].charAt(0)) {
					case '\1':
						fileAlias = splits[j].substring(1);
						break;
					case '\2':
						fileNameDestination = splits[j].substring(1);
						break;
					}
				}
				fileGroupList.add(GeneralConfig.getFileDescription(fileName, filePath, fileAlias, fileNameDestination)); 
			} else
				error = true;
		}
		return error;
    }

    private static boolean verifyGroups(Iterator<List<Command>> commandGroupIterator, Iterator<List<FileDescriptionI>> fileGroupIterator) {
    	boolean error = false;
		List<Command> commandGroupList = commandGroupIterator.next();
		List<FileDescriptionI> fileGroupList = fileGroupIterator.next();
		if (commandGroupList.isEmpty()) {
			commandGroupIterator.remove();
			fileGroupIterator.remove();
			error = true;
		} else if (fileGroupList.isEmpty()) {
			Iterator<Command> commandIterator = commandGroupList.iterator();
			while (!error && commandIterator.hasNext()) 
				if (!commandIterator.next().isServerCommand()) {
					commandGroupIterator.remove();
					fileGroupIterator.remove();
					error = true;
				}
		} else {	
			Iterator<Command> commandIterator = commandGroupList.iterator();
			while (!error && commandIterator.hasNext()) {
				Command command = commandIterator.next();
				if (command.requiresFileName()) {
    				Iterator<FileDescriptionI> fileIterator = fileGroupList.iterator();
    				while (!error && fileIterator.hasNext()) {
    					FileDescriptionI fileDescription = fileIterator.next();
    					File file = new File(fileDescription.getName());
    					if (file.exists()) {
    						if (!ClientConfig.selector.knowsFileFormat(fileDescription.getName())) {
    							commandGroupIterator.remove();
    							fileGroupIterator.remove();
    							error = true;
    						}
    					} else {
    						commandGroupIterator.remove();
    						fileGroupIterator.remove();
    						error = true;
    					}
    				}
				}
			}
		}
		return error;
    }
    
    private static void printGroupError(int number, String group) {
    	System.out.println("Syntaxe du groupe d'op�ration n�" + number + " incorrecte :");
		System.out.println(group);
    }
    
    private static void printUse() {
    	System.out.println();
    	System.out.println("Le programme ne prend qu'un seul param�tre. Ce param�tre peut contenir plusieurs "
    			+ "groupes d'op�ration. Les groupes d'op�rations sont s�par�s par le symbole ';'. Chaque groupe "
    			+ "d'op�rations contient soit la liste des commandes, soit la liste des commandes suivie de la "
    			+ "liste des fichiers sur lesquels effectuer les commandes s�par�es par le symbole ','.");
		System.out.println("Exemples syntaxe : java hdfs.ClientHDFS '<command>'");
		System.out.println("                   java hdfs.ClientHDFS '<command>, <fileName>'");
		System.out.println("                   java hdfs.ClientHDFS '<command>, <fileName>; ...' ");
    	System.out.println();
    }
    
    private static void printCommandUse() {
    	System.out.println();
    	System.out.println("Commandes-----------------------------------------------------------------------------");
    	System.out.println("Liste des commandes : commandes effectuant un traitement sur des fichiers");
    	System.out.println("                      1 <-> upload : envoi d'un fichier sur le serveur");
    	System.out.println("                      2 <-> download : r�cup�ration d'un fichier enregistr� sur le serveur");
    	System.out.println("                      3 <-> delete : suppression d'un fichier enregistr� sur le serveur");
    	System.out.println("                      4 <-> update : mise � jour de la description d'un fichier sur le serveur");
    	System.out.println("                      commandes effectuant un traitement sur le serveur");
    	System.out.println("                      5 <-> status : r�cup�ration des descriptions des fichiers enregistr�s sur le serveur");
    	System.out.println("                      6 <-> verify : v�rification et r�paration au besoin des donn�es des fichiers sur le serveur");
    	System.out.println("Plusieurs commandes peuvent �tre effectu�es sur les fichiers pass�s en argument");
    	System.out.println("Les commandes effectuant un traitement sur le serveur n'ont pas besoin de fichiers en argument");
    	System.out.println("Exemples syntaxe : java hdfs.ClientHDFS '1, <fileName>'");
    	System.out.println("                   java hdfs.ClientHDFS 5");
    	System.out.println("                   java hdfs.ClientHDFS '2 3, <fileName>'");
    	System.out.println("                   java hdfs.ClientHDFS '1, <fileName>; 5; 3, <fileName>'");
    	System.out.println("--------------------------------------------------------------------------------------");
    	System.out.println();
    }
    
    private static void printFileUse() {
    	System.out.println();
    	System.out.println("Descriptions de fichiers--------------------------------------------------------------");
    	System.out.println("Les noms de fichiers peuvent �tre des chemins absolus ou relatifs");
    	System.out.println("Les commandes pass�es en argument peuvent �tre effectu�es sur plusieurs fichiers");
    	System.out.println("Un fichier peut �tre d�crit en plus de par son nom par un alias et par un nom de destination");
    	System.out.println();
    	System.out.println("L'alias permet de d�signer un fichier sur le serveur. Les alias sont uniques et le serveur ne "
    			+ "peut pas avoir plusieurs fichiers avec le m�me alias. Le serveur peut par contre avoir plusieurs fichiers "
    			+ "avec le m�me nom et des alias diff�rents. Dans ce dernier cas, il faut d�signer le fichier sur lequel "
    			+ "effectuer le traitement par son alias respectif. Lorsque la description d'un fichier comprend un alias, "
    			+ "c'est ce dernier qui sera utilis� en priorit� pour trouver le fichier sur le serveur sur lequel effectu� "
    			+ "le traitement.");
    	System.out.println("Seul le premier alias est enregistr� avec le fichier lorsque le celui-ci est envoy� sur le "
    			+ "serveur (commande 1). Par la suite l'alias peut �tre modifi� en mettant � jour la description du fichier "
    			+ "sur le serveur (commande 4). L'alias peut aussi servir � modifier le nom du fichier sur le serveur "
    			+ "(commande 4). Le fichier associ� peut ensuite �tre r�cup�r� (commande 2) en utilisant l'alias � la place "
    			+ "du nom initial du fichier.");
    	System.out.println("L'alias ne peut pas �tre utilis� pour d�signer un fichier lorsque les commandes � effectuer "
    			+ "dessus sont des commandes qui ont besoin du nom effectif du fichier (commande 1).");
    	System.out.println("Exemple s�quentiel de l'utilisation de l'alias");
    	System.out.println("java hdfs.ClientHDFS '1, test.txt -a test'      - Enregistrement de l'alias 'test' avec le fichier test.txt");
    	System.out.println("java hdfs.ClientHDFS '1, test.txt -a testBis'   - Alias 'testBis' ignor�");
    	System.out.println("java hdfs.ClientHDFS '2, test'                  - R�cup�ration du fichier test.txt avec l'alias 'test'");
    	System.out.println("java hdfs.ClientHDFS | '4, test.txt -a testBis' - Mise � jour de l'alias du fichier test.txt sur le serveur");
    	System.out.println("                     | '4, test -a testBis'");
    	System.out.println("java hdfs.ClientHDFS '4, test2.txt -a testBis'  - Mise � jour du nom du fichier test.txt sur le serveur");
    	System.out.println("java hdfs.ClientHDFS '2, testBis'               - R�cup�ration du fichier test.txt avec l'alias 'testBis'");
    	System.out.println();
    	System.out.println("Le nom de destination correspond au nom sous lequel sera enregistr� le fichier lors de sa r�cup�ration "
    			+ "depuis le serveur (commande 2)");
    	System.out.println("Exemple s�quentiel de l'utilisation du nom de destination");
    	System.out.println("java hdfs.ClientHDFS '1, test.txt'                  - Enregistrement du fichier test.txt sur le serveur");
    	System.out.println("java hdfs.ClientHDFS '2, test.txt -d testCopie.txt' - R�cup�ration du fichier test.txt sous le nom testCopie.txt");
    	System.out.println();
    	System.out.println("Exemples syntaxe : java hdfs.ClientHDFS '<command>, test.txt'");
    	System.out.println("                   java hdfs.ClientHDFS '<command>, test1.txt test2.txt'");
    	System.out.println("                   java hdfs.ClientHDFS '<command>, test.txt -a test'");
    	System.out.println("                   java hdfs.ClientHDFS '<command>, test.txt -d testCopie.txt'");
    	System.out.println("                   java hdfs.ClientHDFS '<command>, test.txt -a test -d testCopie.txt'");
    	System.out.println("                   java hdfs.ClientHDFS '<command>, test.txt -d testCopie.txt -a test'");
    	System.out.println("                   java hdfs.ClientHDFS '<command>, test1.txt; <command>, test2.txt'");
    	System.out.println("--------------------------------------------------------------------------------------");
    	System.out.println();
    }
    
}
