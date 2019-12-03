package hdfs;

import java.io.File;
import java.io.FileReader;
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
            convertisseur.putInt(Integer.parseInt(args[0]));
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
    		
    		// Envoie du contenu du fichier
    		File fichier = new File(args[1]); 
    		FileReader reader = new FileReader(fichier);
    		int nbLus;
    		char[] bufferFichier = new char[1024];
    		convertisseur = ByteBuffer.allocate(2048);
    		while ((nbLus = reader.read(bufferFichier, 0, 1024)) > 0) {
    			for (int i = 0; i < nbLus; i++) {
    				convertisseur.putChar(bufferFichier[i]);
    			}
    			buffer = convertisseur.array();
    			convertisseur.clear();
        		serveurOS.write(buffer, 0, 2*nbLus);
    		} 
    		reader.close();
    		serveur.close();
    		serveurOS.close();
    		serveurIS.close();
        } catch (Exception e) {e.printStackTrace();}
    }
}
