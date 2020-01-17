package hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class ClientHDFS {

    public static void main(String[] args) {
        try {
        	
        	// Connexion avec le serveur
            Socket serveur = new Socket("localhost", 8080);
            OutputStream serveurOS = serveur.getOutputStream();
            InputStream serveurIS = serveur.getInputStream();
            ByteBuffer convertisseur = ByteBuffer.allocate(Project.BytesInt);
            
            // Envoie de la commande
            int commande = Integer.parseInt(args[0]);
            convertisseur.putInt(commande);
            byte[] buffer = convertisseur.array();
    		serveurOS.write(buffer, 0, buffer.length);
    		
    		// Envoie du nom du fichier
    		String nomFichier = args[1];
    		convertisseur.clear();
    		convertisseur.putInt(nomFichier.length());
            buffer = convertisseur.array();
    		serveurOS.write(buffer, 0, buffer.length);
            buffer = nomFichier.getBytes();
    		serveurOS.write(buffer, 0, buffer.length);
    		
    		try {
	    		if (commande == 1) {
	    			hdfsWrite(serveurOS, nomFichier);
	    		} else if (commande == 2) {
	    			hdfsRead(serveurIS, nomFichier);
	    		} 
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    		
    		// Déconnexion
    		serveur.close();
    		serveurOS.close();
    		serveurIS.close();
        } catch (Exception e) {e.printStackTrace();}
    }
    
    public static void hdfsWrite(OutputStream serveurOS, String nomFichier) throws IOException {
    	FileInputStream reader = new FileInputStream(nomFichier);
		int nbLus;
		byte[] buffer = new byte[1024];
		while ((nbLus = reader.read(buffer)) > 0) {
    		serveurOS.write(buffer, 0, nbLus);
		} 
		reader.close();
    }
    
    public static void hdfsRead(InputStream serveurIS, String nomFichier) throws IOException {
    	FileOutputStream writer = new FileOutputStream(nomFichier);
		int nbLus;
		byte[] buffer = new byte[1024];
		while ((nbLus = serveurIS.read(buffer)) > 0) {
    		writer.write(buffer, 0, nbLus);
		} 
		writer.close();
    }
    
}
