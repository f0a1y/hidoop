package hdfs.daemon;

import java.net.*;
import java.nio.ByteBuffer;
import java.io.*;
import java.util.*;

import config.Project;
import formats.Format.OpenMode;
import formats.KV;
import formats.LineFormat;

public class DaemonHDFS extends Thread {

    static HashMap<String, List<Integer>> registre;
    static Random rand = new Random();
    static int identifiant;
    private Socket emetteur;
    

    public DaemonHDFS(Socket emetteur) {
        super();
        this.emetteur = emetteur;
    }
    
    public void run() {
        try {
        	
        	// Connexion avec le client 
            OutputStream emetteurOS = this.emetteur.getOutputStream();
            InputStream emetteurIS = this.emetteur.getInputStream();
            
            // Réception de la commande
            byte[] buffer = new byte[Project.BytesInt];
            emetteurIS.read(buffer, 0, Project.BytesInt);
            ByteBuffer convertisseur = ByteBuffer.wrap(buffer);
            int commande = convertisseur.getInt();
            
            // Réception de la taille du nom du fichier
            emetteurIS.read(buffer, 0, Project.BytesInt);
            convertisseur.clear();
            convertisseur = ByteBuffer.wrap(buffer);
            int tailleNomFichier = convertisseur.getInt();
            
            // Réception du nom du fichier
            buffer = new byte[tailleNomFichier];
            emetteurIS.read(buffer, 0, tailleNomFichier);
            String nomFichier = new String(buffer) + this.identifiant;
            
            if (commande == 1) {
            	this.hdfsWrite(nomFichier, emetteurIS);
            } else if (commande == 2) {
            	this.hdfsRead(nomFichier,  emetteurOS);
            } else if (commande == 3) {
            	this.hdfsDelete(nomFichier);
            }
            
            // Déconnexion
            emetteurOS.close();
            emetteurIS.close();
        } catch (Exception e) {e.printStackTrace();}
    }
    
    private void hdfsWrite(String nomFichier, InputStream emetteurIS) throws IOException {
    	
    	// Liste de numéros des fragments du fichier
    	List<Integer> numerosFragments = registre.get(nomFichier);
		if (numerosFragments == null) {
	        numerosFragments = new ArrayList<>();
	        registre.put(nomFichier, numerosFragments);
	    } else {
			numerosFragments.clear();
	    }
	    
	    // Dossier des fragments du fichier
	    String nomDossier = Project.PATH + "data/" + nomFichier + "_" + identifiant + "/";
	    File dossier = new File(nomDossier);
		dossier.mkdir();
		
        // Réception du contenu du fichier
		byte[] buffer = new byte[1024];
        ByteBuffer convertisseur;
        int nbLus, taille, ordre;
    	while (emetteurIS.read(buffer, 0, Project.BytesInt) > 0) {
            
            // Réception de l'ordre du fragment
            convertisseur = ByteBuffer.wrap(buffer);
            ordre = convertisseur.getInt();
            numerosFragments.add(ordre);
            convertisseur.clear();
            
            // Réception de la taille du fragment
            emetteurIS.read(buffer, 0, Project.BytesInt);
            convertisseur = ByteBuffer.wrap(buffer);
            taille = convertisseur.getInt();
            convertisseur.clear();
            
            // Réception du fragment
            StringBuilder texte = new StringBuilder();
            while (taille > 0) {
            	nbLus = emetteurIS.read(buffer, 0, Math.min(1024, taille));
            	taille -= nbLus;
            	texte.append(new String(buffer, 0, nbLus));
            }
            
            // Ecriture du fragment
            LineFormat editeur = new LineFormat(nomDossier + ordre);
			editeur.open(OpenMode.W);
            editeur.write(new KV("0", texte.toString()));
			editeur.close();
            texte.setLength(0);
    	}
    }
    
    private void hdfsRead(String nomFichier, OutputStream emetteurOS) throws IOException {
	    File dossier = new File(Project.PATH + "data/" + nomFichier + "_" + identifiant + "/");
	    
	    // Lecture des fragments
	    if (dossier.exists()) {
			for (File fichier : dossier.listFiles()) {
				
				// Lecture du contenu du fichier
				LineFormat lecteur = new LineFormat(fichier.getAbsolutePath());
				lecteur.open(OpenMode.R);
				KV fragment = lecteur.read();
				envoyerTexte(emetteurOS, fragment.k);
				envoyerTexte(emetteurOS, fragment.v);
				lecteur.close();
			}
		}
    }
    
    private void envoyerTexte(OutputStream emetteurOS, String texte) throws IOException {
        ByteBuffer convertisseur = ByteBuffer.allocate(Project.BytesInt);
        byte[] bufferTexte = texte.getBytes();
		convertisseur.putInt(bufferTexte.length);
		byte[] bufferTaille = convertisseur.array();
        emetteurOS.write(bufferTaille);
		emetteurOS.write(bufferTexte);
    }
    
    private void hdfsDelete(String nomFichier) {
    	File dossier = new File(Project.PATH + "data/" + nomFichier + "_" + identifiant + "/");
	    if (dossier.exists()) {
			for (File fichier : dossier.listFiles()) {
				fichier.delete();
			}
		}
    	dossier.delete();
    }

    public static void main(String[] args) {
        try {
        	if (args.length == 1) {
	            identifiant = Integer.parseInt(args[0]);
	            
				// Création de la collection contenant les numéros des fragments par fichier
				File fichier = new File("registre" + identifiant + ".ser"); 
				if (fichier.exists()) {
					try {
						ObjectInputStream objectIS = new ObjectInputStream(new FileInputStream(fichier));
						registre = (HashMap<String, List<Integer>>)objectIS.readObject();
						objectIS.close();
					} catch(IOException e) {
						e.printStackTrace();
						return;
					}
				} else {
					registre = new HashMap<>();
				}
	            
	            ServerSocket client = new ServerSocket(Project.numPortHDFS[identifiant]);
	            while (true) {
	                DaemonHDFS daemon = new DaemonHDFS(client.accept());
	                daemon.start();
	            }
        	} else {
        		System.out.println("Usage : java hdfs.daemon.DaemonHDFS <identifiant>");
        	}
        } catch (Exception e) {e.printStackTrace();}
    }

}
