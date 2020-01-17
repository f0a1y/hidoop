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

    static Random rand = new Random();
    private Socket emetteur;
    private int identifiant;

    public DaemonHDFS(Socket emetteur, int identifiant) {
        super();
        this.emetteur = emetteur;
        this.identifiant = identifiant;
    }

    public void run() {
        try {
        	
        	// Connexion avec le serveur
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
    	LineFormat editeur = new LineFormat(nomFichier);
    	editeur.open(OpenMode.W);
    	
        // Réception du contenu du fichier
		byte[] buffer = new byte[1024];
        ByteBuffer convertisseur;
        int nbLus, taille, ordre;
    	while ((nbLus = emetteurIS.read(buffer, 0, Project.BytesInt)) > 0) {
            
            //Réception de la taille du fragment
            convertisseur = ByteBuffer.wrap(buffer);
            taille = convertisseur.getInt();
            convertisseur.clear();
            
            //Réception du fragment
            StringBuilder texte = new StringBuilder();
            while (taille > 0) {
            	nbLus = emetteurIS.read(buffer, 0, Math.min(1024, taille));
            	taille -= nbLus;
            	texte.append(new String(buffer, 0, nbLus));
            }
            editeur.write(new KV("0", texte.toString()));
            
            texte.setLength(0);
    	}
    	editeur.close();
    }
    
    private void hdfsRead(String nomFichier, OutputStream emetteurOS) throws IOException {
    	File fichier = new File(nomFichier); 
    	if (fichier.exists()) {
    		LineFormat lecteur = new LineFormat(nomFichier);
    		lecteur.open(OpenMode.R);

    		// Lecture du contenu du fichier
    		KV fragment;
    		while ((fragment = lecteur.read()) != null){
    			envoyerTexte(emetteurOS, fragment.k);
    			envoyerTexte(emetteurOS, fragment.v);
    		}
    		lecteur.close();
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
    	File fichier = new File(nomFichier);
    	fichier.delete();
    }

    public static void main(String[] args) {
        try {
        	if (args.length == 1) {
	            int identifiant = Integer.parseInt(args[0]);
	            ServerSocket client = new ServerSocket(Project.numPortHDFS[identifiant]);
	            while (true) {
	                DaemonHDFS daemon = new DaemonHDFS(client.accept(), identifiant);
	                daemon.start();
	            }
        	} else {
        		System.out.println("Usage : java hdfs.daemon.DaemonHDFS <identifiant>");
        	}
        } catch (Exception e) {e.printStackTrace();}
    }

}
