package hdfs;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class ClientHDFS {

    public static void main(String[] args) {
        try {
            Socket serveur = new Socket("localhost", 8080);
            OutputStream serveurOS = serveur.getOutputStream();
            InputStream serveurIS = serveur.getInputStream();
            ByteBuffer convertisseur = ByteBuffer.allocate(Integer.SIZE/Byte.SIZE);
            
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
    		serveur.close();
    		serveurOS.close();
    		serveurIS.close();
        } catch (Exception e) {e.printStackTrace();}
    }
    
    public static void hdfsWrite(OutputStream serveurOS, String nomFichier) throws IOException {
		File fichier = new File(nomFichier); 
		FileReader reader = new FileReader(fichier);
		int nbLus;
		byte[] buffer = new byte[1024];
		char[] bufferFichier = new char[1024];
		String contenu;
		while ((nbLus = reader.read(bufferFichier, 0, 1024)) > 0) {
			contenu = new String(bufferFichier, 0, nbLus);
			buffer = contenu.getBytes();
    		serveurOS.write(buffer, 0, buffer.length);
		} 
		reader.close();
    }
    
    public static void hdfsRead(InputStream serveurIS, String nomFichier) throws IOException {
    	File fichier = new File(nomFichier); 
		FileWriter writer = new FileWriter(fichier);
		int nbLus;
		byte[] buffer = new byte[1024];
		String contenu;
		while ((nbLus = serveurIS.read(buffer)) > 0) {
    		contenu = new String(buffer, 0, nbLus);
    		writer.write(contenu);
		} 
		writer.close();
    }
    
}
