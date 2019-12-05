package hdfs;

import java.net.*;
import java.nio.ByteBuffer;
import java.io.*;
import java.util.*;

public class DaemonHDFS extends Thread {

    static Random rand = new Random();
    private Socket emetteur;
    static private HashMap<String, List<FragmentTexte>> donnee = new HashMap<>();

    public DaemonHDFS(Socket emetteur) {
        super();
        this.emetteur = emetteur;
    }

    public void run() {
        try {
            OutputStream emetteurOS = this.emetteur.getOutputStream();
            InputStream emetteurIS = this.emetteur.getInputStream();
            
            // Reception de la commande
            byte[] buffer = new byte[Integer.SIZE/Byte.SIZE];
            emetteurIS.read(buffer, 0, Integer.SIZE/Byte.SIZE);
            ByteBuffer convertisseur = ByteBuffer.wrap(buffer);
            int commande = convertisseur.getInt();
            
            // Reception de la taille du nom du fichier
            emetteurIS.read(buffer, 0, Integer.SIZE/Byte.SIZE);
            convertisseur.clear();
            convertisseur = ByteBuffer.wrap(buffer);
            int tailleNomFichier = convertisseur.getInt();
            
            // Reception du nom du fichier
            buffer = new byte[tailleNomFichier];
            emetteurIS.read(buffer, 0, tailleNomFichier);
            String nomFichier = new String(buffer);
            
            if (commande == 1) {
            	this.hdfsWrite(nomFichier, emetteurIS);
            } else if (commande == 2) {
            	this.hdfsRead(nomFichier,  emetteurOS);
            } else if (commande == 3) {
            	this.hdfsDelete(nomFichier);
            }
            emetteurOS.close();
            emetteurIS.close();
        } catch (Exception e) {e.printStackTrace();}
    }
    
    private void hdfsWrite(String nomFichier, InputStream emetteurIS) throws IOException {
        // Création de la liste des fragments pour le fichier
        if (!donnee.containsKey("nomFichier")) {
        	System.out.println("création liste");
        	donnee.put(nomFichier, new ArrayList<>());
        }
        
        // Reception du contenu du fichier
		byte[] buffer = new byte[1024];
        ByteBuffer convertisseur;
        int nbLus, reste;
    	while ((nbLus = emetteurIS.read(buffer, 0, Integer.SIZE/Byte.SIZE)) > 0) {
            convertisseur = ByteBuffer.wrap(buffer);
            int ordre = convertisseur.getInt();
            convertisseur.clear();
            nbLus = emetteurIS.read(buffer, 0, Integer.SIZE/Byte.SIZE);
            convertisseur = ByteBuffer.wrap(buffer);
            reste = convertisseur.getInt();
            convertisseur.clear();
            String texte = "";
            while (reste > 0) {
            	nbLus = emetteurIS.read(buffer, 0, Math.min(1024, reste));
            	reste -= nbLus;
            	texte += new String(buffer, 0, nbLus);
            }
            System.out.println(texte);
            donnee.get(nomFichier).add(new FragmentTexte(ordre, texte));
    	}
    	
    }
    
    private void hdfsRead(String nomFichier, OutputStream emetteurOS) throws IOException {
    	List<FragmentTexte> fragments = donnee.get(nomFichier);
    	/*ObjectOutputStream emetteurOOS = new ObjectOutputStream(emetteurOS);
    	for (FragmentTexte fragment : fragments) {
    		emetteurOOS.writeObject(fragment);    	
    	}*/
    	byte[] buffer;
        ByteBuffer convertisseur = ByteBuffer.allocate(Integer.SIZE/Byte.SIZE);
    	for (FragmentTexte fragment : fragments) {
    		convertisseur.clear();
    		convertisseur.putInt(fragment.getOrdre());
            buffer = convertisseur.array();
            emetteurOS.write(buffer);
    		convertisseur.clear();
    		String texte = fragment.getTexte();
    		convertisseur.putInt(texte.length());
            buffer = convertisseur.array();
            emetteurOS.write(buffer);
            buffer = texte.getBytes();
    		emetteurOS.write(buffer);
    	}
    }
    
    private void hdfsDelete(String nomFichier) {
    	donnee.remove(nomFichier);
    }

    public static void main(String[] args) {
        try {
            ServerSocket client = new ServerSocket(Integer.parseInt(args[0]));
            while (true) {
                DaemonHDFS daemon = new DaemonHDFS(client.accept());
                daemon.start();
            }
        } catch (Exception e) {e.printStackTrace();}
    }

}