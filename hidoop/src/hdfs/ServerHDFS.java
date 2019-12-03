package hdfs;

import java.net.*;
import java.nio.ByteBuffer;
import java.io.*;
import java.util.*;

public class ServerHDFS extends Thread {

    static String hosts[] = {"localhost"};
    static int ports[] = {8081};
    static int nbNodes = 1;
    static Random rand = new Random();
    private Socket emetteur;

    public ServerHDFS(Socket emetteur) {
        super();
        this.emetteur = emetteur;
    }

    public void run() {
        try {
            OutputStream emetteurOS = this.emetteur.getOutputStream();
            InputStream emetteurIS = this.emetteur.getInputStream();
            Socket[] nodes = new Socket[nbNodes];
            OutputStream[] recepteursOS = new OutputStream[3];
            InputStream[] recepteursIS = new InputStream[3];
            for (int i = 0; i < nbNodes; i++) {
	            nodes[i] = new Socket(hosts[i], ports[i]);
	            recepteursOS[i] = nodes[i].getOutputStream();
	            recepteursIS[i] = nodes[i].getInputStream();
            }
            
            // Reception commande
            byte[] buffer = new byte[Integer.SIZE/Byte.SIZE];
            int nbLus = emetteurIS.read(buffer, 0, Integer.SIZE/Byte.SIZE);
            ByteBuffer convertisseur = ByteBuffer.wrap(buffer);
            int commande = convertisseur.getInt(); 
            
            // Commande WriteHDFS
            if (commande == 1) {
            	
            	// Envoie de la commande aux noeuds du cluster
            	for (int i = 0; i< nbNodes; i++) {
            		recepteursOS[i].write(buffer, 0, nbLus);
            	}
            	
            	// Envoie du nom du fichier aux noeuds du cluster
                nbLus = emetteurIS.read(buffer, 0, Integer.SIZE/Byte.SIZE);
                convertisseur.clear();
                convertisseur = ByteBuffer.wrap(buffer);
                int tailleNomFichier = convertisseur.getInt();
            	for (int i = 0; i< nbNodes; i++) {
            		recepteursOS[i].write(buffer, 0, buffer.length);
            	}
                buffer = new byte[tailleNomFichier];
                nbLus = emetteurIS.read(buffer, 0, tailleNomFichier);
            	for (int i = 0; i< nbNodes; i++) {
            		recepteursOS[i].write(buffer, 0, nbLus);
            	}
            	
            	// Reception du contenu du fichier
            	buffer = new byte[1024];
            	String fichier, fragment = "";
            	String[] fragments;
            	int ordre = 0;
            	while ((nbLus = emetteurIS.read(buffer)) > 0) {
            		fichier = new String(buffer, 0, nbLus);
            		fragment = fragment + fichier;
            		fragments = fragment.split("\n", 2);
            		if (fragments.length == 2) {
                		convertisseur.clear();
                		convertisseur.putInt(ordre);
                        buffer = convertisseur.array();
                        int node = rand.nextInt(nbNodes);
                        recepteursOS[node].write(buffer, 0, buffer.length);
                        buffer = fragments[0].getBytes();
                		convertisseur.clear();
                		convertisseur.putInt(buffer.length);
                        buffer = convertisseur.array();
                        recepteursOS[node].write(buffer, 0, buffer.length);
                		recepteursOS[node].write(buffer, 0, buffer.length);
                		ordre++;
                		fragment = fragments[1];
            		} else {
            			fragment = fragments[0];
            		}
            	}
               
            }
            emetteurOS.close();
            emetteurIS.close();
            for (int i = 0; i < nbNodes; i++) {
            	nodes[i].close();
            	recepteursOS[i].close();
            	recepteursIS[i].close();
            }
        } catch (Exception e) {e.printStackTrace();}
    }

    public static void main(String[] args) {
        try {
            ServerSocket client = new ServerSocket(8080);
            while (true) {
                ServerHDFS serveur = new ServerHDFS(client.accept());
                serveur.start();
            }
        } catch (Exception e) {e.printStackTrace();}
    }

} 
