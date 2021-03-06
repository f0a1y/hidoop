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
        		recepteursOS[i].write(buffer, 0, nbLus);
        	}
            buffer = new byte[tailleNomFichier];
            nbLus = emetteurIS.read(buffer, 0, tailleNomFichier);
        	for (int i = 0; i< nbNodes; i++) {
        		recepteursOS[i].write(buffer, 0, nbLus);
        	}
        	
            // Commande WriteHDFS
            if (commande == 1) {
               this.hdfsWrite(emetteurIS, recepteursOS);
            } else if (commande == 2) {
            	this.hdfsRead(emetteurOS, recepteursIS);
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
    
    public void hdfsWrite(InputStream emetteurIS, OutputStream[] recepteursOS) throws IOException {
    	int nbLus;
    	byte[] buffer = new byte[1024];
        ByteBuffer convertisseur = ByteBuffer.allocate(Integer.SIZE/Byte.SIZE);
    	String fichier = "";
    	int ordre = 0;
    	while ((nbLus = emetteurIS.read(buffer)) > 0) {
    		fichier += new String(buffer, 0, nbLus);
    		int ligne;
    		while ((ligne = fichier.indexOf('\n')) >= 0) {
                int node = rand.nextInt(nbNodes);
        		convertisseur.clear();
        		convertisseur.putInt(ordre);
                buffer = convertisseur.array();
                recepteursOS[node].write(buffer);
        		convertisseur.clear();
        		String fragment = fichier.substring(0, ligne + 1);
        		convertisseur.putInt(fragment.length());
                buffer = convertisseur.array();
                recepteursOS[node].write(buffer);
                buffer = fragment.getBytes();
        		recepteursOS[node].write(buffer);
        		fichier = fichier.substring(ligne + 1, fichier.length());
        		ordre++;
    		}
    	}
    }
    
    public void hdfsRead(OutputStream emetteurOS, InputStream[] recepteursIS) throws IOException {
		HashMap<Integer, String> fragments = new HashMap<>();
    	byte[] buffer = new byte[1024];
        ByteBuffer convertisseur;
        int nbLus, reste, courant = 0;
        for (int i = 0; i < nbNodes; i++) {
	    	while ((nbLus = recepteursIS[i].read(buffer, 0, Integer.SIZE/Byte.SIZE)) > 0) {
	            convertisseur = ByteBuffer.wrap(buffer);
	            int ordre = convertisseur.getInt();
	            convertisseur.clear();
	            nbLus = recepteursIS[i].read(buffer, 0, Integer.SIZE/Byte.SIZE);
	            convertisseur = ByteBuffer.wrap(buffer);
	            reste = convertisseur.getInt();
	            convertisseur.clear();
	            String texte = "";
	            while (reste > 0) {
	            	nbLus = recepteursIS[i].read(buffer, 0, Math.min(1024, reste));
	            	reste -= nbLus;
	            	texte += new String(buffer, 0, nbLus);
	            }
	            fragments.put(ordre, texte);
	            while (fragments.containsKey(courant)) {
	            	byte[] bufferFichier = fragments.get(courant).getBytes();
	            	fragments.remove(courant);
	            	courant++;
	            	emetteurOS.write(bufferFichier);
	            }
	    	}
        }
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
