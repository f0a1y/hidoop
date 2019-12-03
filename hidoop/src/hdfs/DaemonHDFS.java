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
            byte[] buffer = new byte[Integer.SIZE/Byte.SIZE];
            int nbLus = emetteurIS.read(buffer, 0, Integer.SIZE/Byte.SIZE);
            ByteBuffer convertisseur = ByteBuffer.wrap(buffer);
            int commande = convertisseur.getInt();
            System.out.println(commande);
            nbLus = emetteurIS.read(buffer, 0, Integer.SIZE/Byte.SIZE);
            convertisseur.clear();
            convertisseur = ByteBuffer.wrap(buffer);
            int tailleNomFichier = convertisseur.getInt();
            System.out.println(tailleNomFichier);
            buffer = new byte[tailleNomFichier];
            nbLus = emetteurIS.read(buffer, 0, tailleNomFichier);
            String nomFichier = new String(buffer);
            System.out.println(nomFichier);
            if (!this.donnee.containsKey("nomFichier")) {
            	this.donnee.put(nomFichier, new ArrayList<>());
            }
        	while ((nbLus = emetteurIS.read(buffer, 0, Integer.SIZE/Byte.SIZE)) > 0) {
                convertisseur.clear();
                convertisseur = ByteBuffer.wrap(buffer);
                int ordre = convertisseur.getInt();
                System.out.println(ordre);
                nbLus = emetteurIS.read(buffer, 0, Integer.SIZE/Byte.SIZE);
                convertisseur.clear();
                convertisseur = ByteBuffer.wrap(buffer);
                int tailleTexte = convertisseur.getInt();
                System.out.println(tailleTexte);
                nbLus = emetteurIS.read(buffer, 0, tailleTexte);
                String texte = new String(buffer);
                this.donnee.get(nomFichier).add(new FragmentTexte(ordre, texte));
        	}
            emetteurOS.close();
            emetteurIS.close();
        } catch (Exception e) {e.printStackTrace();}
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