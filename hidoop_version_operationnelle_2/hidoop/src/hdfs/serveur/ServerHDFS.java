package hdfs.serveur;

import java.net.*;
import java.nio.ByteBuffer;
import java.io.*;
import java.util.*;

import config.Project;
import formats.FormatWriter;
import formats.KV;

public class ServerHDFS extends Thread {

    static HashMap<String, List<Integer>> registre;
    static ChoixFragmenteurBasique choix;
    static Random rand = new Random();
    private Socket emetteur;

    public ServerHDFS(Socket emetteur) {
        super();
        this.emetteur = emetteur;
    }

    public void run() {
        try {
        	
        	// Connexion avec les noeuds du cluster
            OutputStream emetteurOS = this.emetteur.getOutputStream();
            InputStream emetteurIS = this.emetteur.getInputStream();
            Socket[] nodes = new Socket[Project.nbMachine];
            OutputStream[] recepteursOS = new OutputStream[Project.nbMachine];
            InputStream[] recepteursIS = new InputStream[Project.nbMachine];
            for (int i = 0; i < Project.nbMachine; i++) {
                nodes[i] = new Socket(Project.nomMachine[i+1], Project.numPortHDFS[i+1]);
                recepteursOS[i] = nodes[i].getOutputStream();
                recepteursIS[i] = nodes[i].getInputStream();
            }
            
            // Réception commande
            byte[] bufferCommande = new byte[Project.BytesInt];
            emetteurIS.read(bufferCommande, 0, Project.BytesInt);
            ByteBuffer convertisseur = ByteBuffer.wrap(bufferCommande);
            int commande = convertisseur.getInt(); 
        	
            // Réception du nom du fichier
            byte[] bufferTailleNomFichier = new byte[Project.BytesInt];
            emetteurIS.read(bufferTailleNomFichier, 0, Project.BytesInt);
            convertisseur.clear();
            convertisseur = ByteBuffer.wrap(bufferTailleNomFichier);
            int tailleNomFichier = convertisseur.getInt();
            byte[] bufferNomFichier = new byte[tailleNomFichier];
            emetteurIS.read(bufferNomFichier, 0, tailleNomFichier);
            String nomFichier = new String(bufferNomFichier);
            
            // Récupération du fragmenteur adapté au format du fichier;
            Fragmenteur fragmenteur = choix.choisirFragmenteur(nomFichier);
            if (fragmenteur == null) {
            	System.out.println("format du choix inconnu");
            } else {
            	
	        	// Envoie de la commande aux noeuds du cluster
	        	for (int i = 0; i < Project.nbMachine; i++) {
	        		recepteursOS[i].write(bufferCommande, 0, Project.BytesInt);
	        	}
	
	        	// Envoie du nom du fichier aux noeuds du cluster
	        	for (int i = 0; i< Project.nbMachine; i++) {
	        		recepteursOS[i].write(bufferTailleNomFichier, 0, Project.BytesInt);
	        	}
	        	for (int i = 0; i< Project.nbMachine; i++) {
	        		recepteursOS[i].write(bufferNomFichier, 0, tailleNomFichier);
	        	}
	        	
	            if (commande == 1) {
	            	
	            	// Création de la liste contenant l'ordre d'envoi des fragments dans les noeuds
	            	List<Integer> ordre = registre.get(nomFichier);
	            	if (ordre == null) {
	            		ordre = new ArrayList<>();
	            		registre.put(nomFichier, ordre);
	            	} else {
		            	ordre.clear();
	            	}
	            	this.hdfsWrite(emetteurIS, recepteursOS, fragmenteur, ordre);
	            } else if (commande == 2) {
	            	
	            	// Récupération de la liste contenant l'ordre d'envoi des fragments dans les noeuds
	            	if (registre.containsKey(nomFichier)) {
	            		this.hdfsRead(emetteurOS, recepteursIS, registre.get(nomFichier));
	            	} else {
	            		System.out.println("Le fichier " + nomFichier + "n'est pas enregistré sur le serveur");
	            	}
	            } else if (commande == 3) {
	            	registre.remove(nomFichier);
	            }
	            
	            // Déconnexion
	            emetteurOS.close();
	            emetteurIS.close();
	            for (int i = 0; i < Project.nbMachine; i++) {
	            	nodes[i].close();
	            	recepteursOS[i].close();
	            	recepteursIS[i].close();
	            }
            }
        } catch (Exception e) {e.printStackTrace();}
    }
    
    private void hdfsWrite(InputStream emetteurIS, OutputStream[] recepteursOS, Fragmenteur fragmenteur, List<Integer> ordre) throws IOException {
    	int nbLus;
    	byte[] buffer = new byte[fragmenteur.getTailleFragment()];
    	ByteBuffer stock = ByteBuffer.allocate(fragmenteur.getTailleFragment() * 3);
    	
    	// Réception du contenu du fichier
    	while ((nbLus = emetteurIS.read(buffer)) > 0) {
    		stock.put(buffer, 0, nbLus);
    		
    		// Fragmentation du contenu du fichier reçu
    		List<byte[]> fragments = new ArrayList<>();
    		byte[] trame = stock.array();
    		int limite = stock.position();
    		int indice = fragmenteur.fragmenter(trame, limite, fragments);
    		stock.clear();    		
    		stock.put(trame, indice, limite - indice);
    		
    		// Envoi des fragments
    		for (byte[] fragment : fragments) {
                envoyerFragment(recepteursOS, fragment, fragment.length, ordre);
    		}
    	}
    	
    	// Envoi du reste du contenu du fichier
    	if (stock.position() > 0) {
            envoyerFragment(recepteursOS, stock.array(), stock.position(), ordre);
    	}
    	
    	// Enregistrement de la liste contenant l'ordre d'envoi des fragments dans les noeuds
    	try {
            ObjectOutputStream objectOS = new ObjectOutputStream(new FileOutputStream("registre.ser"));
            objectOS.writeObject(registre);
            objectOS.close();
        } catch(IOException e) {
        	e.printStackTrace();
        }
    }
    
    private void envoyerFragment(OutputStream[] recepteursOS, byte[] fragment, int longueur, List<Integer> ordre) throws IOException  {
        ByteBuffer convertisseur = ByteBuffer.allocate(Project.BytesInt);
        int node = rand.nextInt(Project.nbMachine);
		ordre.add(node);
		convertisseur.putInt(longueur);
        recepteursOS[node].write(convertisseur.array());
		recepteursOS[node].write(fragment, 0, longueur);
    }
    
    public void hdfsRead(OutputStream emetteurOS, InputStream[] recepteursIS, List<Integer> ordre) throws IOException {
    	for (Integer i : ordre) {
    		KV fragment = recupererFragment(recepteursIS[i]);
    		byte[] buffer = fragment.v.getBytes();
    		emetteurOS.write(buffer, 0, buffer.length);
    	}
    }
    
    private static KV recupererFragment(InputStream recepteurIS) throws IOException {
    	String cle = recupererTexte(recepteurIS);
    	if (cle != null) {
    		return new KV (cle, recupererTexte(recepteurIS));
    	}
    	return null;
    }
    
    private static String recupererTexte(InputStream recepteurIS) throws IOException {
    	byte[] buffer = new byte[1024];
        int nbLus, taille;
    	if ((nbLus = recepteurIS.read(buffer, 0, Project.BytesInt)) > 0) {
            ByteBuffer convertisseur = ByteBuffer.wrap(buffer);
            taille = convertisseur.getInt();
            convertisseur.clear();
            StringBuilder texte = new StringBuilder();
            while (taille > 0) {
            	nbLus = recepteurIS.read(buffer, 0, Math.min(1024, taille));
            	taille -= nbLus;
            	texte.append(new String(buffer, 0, nbLus));
            }
            return texte.toString();
    	}
    	return null;
    }
    
    public static void recupererResultats(String nomFichier, FormatWriter editeur) {
    	try {

        	// Connexion avec les noeuds du cluster
	    	Socket[] nodes = new Socket[Project.nbMachine];
	        OutputStream[] recepteursOS = new OutputStream[Project.nbMachine];
	        InputStream[] recepteursIS = new InputStream[Project.nbMachine];
	        for (int i = 0; i < Project.nbMachine; i++) {
	            nodes[i] = new Socket(Project.nomMachine[i+1], Project.numPortHDFS[i+1]);
	            recepteursOS[i] = nodes[i].getOutputStream();
	            recepteursIS[i] = nodes[i].getInputStream();
	        }

	    	// Envoie de la commande 2 (hdfsRead) aux noeuds du cluster
	    	ByteBuffer convertisseur = ByteBuffer.allocate(Project.BytesInt);
	    	convertisseur.putInt(2);
	        byte[] buffer = convertisseur.array();
	    	for (int i = 0; i< Project.nbMachine; i++) {
	    		recepteursOS[i].write(buffer, 0, buffer.length);
	    	}
	    	
	    	// Envoie du nom du fichier aux noeuds du cluster
			convertisseur.clear();
			convertisseur.putInt(nomFichier.length());
	        buffer = convertisseur.array();
	    	for (int i = 0; i< Project.nbMachine; i++) {
	    		recepteursOS[i].write(buffer, 0, buffer.length);
	    	}
	        buffer = nomFichier.getBytes();
	    	for (int i = 0; i< Project.nbMachine; i++) {
	    		recepteursOS[i].write(buffer, 0, buffer.length);
	    	}
	    	
	        // Réception des fragments du fichier des noeuds dans l'ordre d'envoi
	        for (int i = 0; i < Project.nbMachine; i++) {
	        	KV fragment;
		    	while ((fragment = recupererFragment(recepteursIS[i])) != null) {
		    		editeur.write(fragment);
		    	}
	        }
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    public static void main(String[] args) {
    	try {

    		// Création de la collection contenant les ordres d'envoi des fragments dans les noeuds par fichier
    		File fichier = new File("registre.ser"); 
    		if (fichier.exists()) {
    			try {
    				ObjectInputStream objectIS = new ObjectInputStream(new FileInputStream("registre.ser"));
    				registre = (HashMap<String, List<Integer>>)objectIS.readObject();
    				objectIS.close();
    			} catch(IOException e) {
    				e.printStackTrace();
    				return;
    			}
    		} else {
    			registre = new HashMap<>();
    		}

    		// Objet permettant de récupérer le fragmenteur adapté au fichier
    		choix = new ChoixFragmenteurBasique(1024);

    		// Attente d'un client
    		ServerSocket client = new ServerSocket(8080);
    		while (true) {
    			ServerHDFS serveur = new ServerHDFS(client.accept());
    			serveur.start();
    		}
    		
    	} catch (Exception e) {e.printStackTrace();}
    }

} 
