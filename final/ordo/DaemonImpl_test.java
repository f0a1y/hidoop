package ordo;

import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject ;

import java.net.InetAddress;

import map.*;
import config.*  ;
import formats.Format;


import formats.Format.OpenMode;
import formats.Format.Type;


public class DaemonImpl extends UnicastRemoteObject implements Daemon {
	
	private int id;


	//constructeur
	public DaemonImpl( int i) throws RemoteException {
		this.id = i;
	
	}
	

	

	//methode distante
	@Override
	public void runMap(final Mapper m, final Format reader, final Format writer, final CallBack cb) throws RemoteException {
		System.out.println(" deamon1");

		// crée un thread secondaire qui execute de map pendant qu'on redonne la main au programme principal
		Thread t = new Thread() {

			public void run() {
				try {
					System.out.println(" deamon4"); 

					mapInterne(m, reader, writer, cb);

					System.out.println(" deamon9");

				} catch (RemoteException e) {
					System.out.println(" deamon_problème sur le Runmap");
					e.printStackTrace();
					
				}
		}
		};

		//lancement du thread secondaire
		System.out.println(" deamon2"); 
		t.start(); 
		System.out.println(" deamon3"); 
		
	}


	public void mapInterne (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		try {
			
			//Ouverture du reader et du writer
			System.out.println(" deamon5"); 
			reader.open(OpenMode.R);
			writer.open(OpenMode.W);
			System.out.println(" deamon6"); 


			//Appel de la fonction map
			m.map(reader, writer);
			System.out.println(" deamon7");
			

			//Fermeture du reader et du writer
			reader.close();
			writer.close();
			System.out.println(" deamon8");
			

			//appel du callback à la fin de l'exécution
			cb.MapFinished();
			System.out.println(" deamon9"); 


		} catch (Exception e) {
			System.out.println(" deamon_erreur sur le mapInterne");
			e.printStackTrace();
		}

	}
	
	public static void main(String args[]) {  
		//argument = indice correspondant à l'ID

			 String machine = new String("vide"); //permet d'initialiser machine dans tout les cas
			 									  // sinon ça ne compile pas

			// récupération de l'id
			 int id = Integer.parseInt(args[0]);


			 //récupération du numéro de port correspondant
			 int port =config.Project.numPortHidoop[id];


			 //récupération du nom complet de la machine surlequel est lancé le daemon
			 try {
			 	 machine = InetAddress.getLocalHost().getHostName();
			 }catch (Exception e) { 
				 e.printStackTrace(); 
				 System.exit(0);
			}


		//creation du serveur de nom
		try {
			Registry registry = LocateRegistry.createRegistry(port);
		} catch (Exception e) {
			System.out.println(" registre deja cree");
		}


		//enregistrement auprès du serveur de nom
		try{
			Naming.rebind("//"+machine+":"+port+"/Daemon", new DaemonImpl(id));
			System.out.println("le Daemon numero "+id+" est lancé sur la machine "+machine+ ", au port "+port);
		
		} catch (Exception e) {
			System.out.println(" probleme sur l'enregistrement auprès du serveur de nom");
			e.printStackTrace();
			System.exit(0);
		}
	}

}
