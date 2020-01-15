package ordo;

import java.rmi.*;
import java.rmi.registry.*;
//import java.rmi.server.useLocalHostname;
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
		// creer un thread secondaire qui execute de map pendant qu'on redonne la main au prgm principal
		Thread t = new Thread() {
			public void run() {
				try {
					System.out.println(" deamon2");
					mapInterne(m, reader, writer, cb);
					System.out.println(" deamon3");
				} catch (RemoteException e) {
					System.out.println(" deamon4");
					e.printStackTrace();
					
				}
		}
		};
		System.out.println(" deamon5");
		t.start(); 
		System.out.println(" deamon6");
		
	}

//string Format reader
	public void mapInterne (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		try {
			
			//redéfinir le reader et le writer pour correspondre à l'architecture d'HDFS

			//Ouverture du reader et du writer
			System.out.println(" deamon7");
			reader.open(OpenMode.R);
			writer.open(OpenMode.W);
			System.out.println(" deamon8");
			//Appel de la fonction map
			m.map(reader, writer);
			System.out.println(" deamon9");
			
			//Fermeture du reader et du writer
			reader.close();
			writer.close();
			System.out.println(" deamon10");
			
			//appel du callback à la fin de l'exécution
			cb.MapFinished();
			System.out.println(" deamon11");
		} catch (Exception e) {
			System.out.println(" deamon12");
			e.printStackTrace();
		}

	}
	
	public static void main(String args[]) {  //argument = indice correspondant au nom du pc

			 String machine = new String("vide");
			 int id = Integer.parseInt(args[0]);
			 int port =config.Project.numPortHidoop[id];
			 //String machine = new String(config.Project.nomMachine[id]);
			 try {
			 	 machine = InetAddress.getLocalHost().getHostName();
				 System.out.println(machine);
			 }catch (Exception e) { e.printStackTrace();}
		try {
			//creation du serveur de nom
			Registry registry = LocateRegistry.createRegistry(port);
		} catch (Exception e) {
			System.out.println(" registre deja cree");
		}
		try{
			//enregistrement auprès du serveur de nom
				//java.rmi.server.useLocalHostname=true;
			Naming.rebind("//"+machine+":"+port+"/Daemon", new DaemonImpl(id));
			System.out.println("le Daemon numero "+id+" est lancé sur la machine "+machine+ ", au port "+port);
		
		} catch (Exception e) {
			System.out.println(" probleme sur l'enregistrement auprès du serveur de nom");
			e.printStackTrace();
		}
	}

}
