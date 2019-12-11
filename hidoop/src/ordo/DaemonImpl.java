package ordo;

import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject ;



import map.*;
import config.*  ;
import formats.Format;


import formats.Format.OpenMode;
import formats.Format.Type;


public class DaemonImpl extends UnicastRemoteObject implements Daemon {
	
	
	//constructeur
	public DaemonImpl() throws RemoteException {
	
	}
	

	

	//methode distante
	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {

		// creer un thread secondaire qui execute de map pendant qu'on redonne la main au prgm principal
		Thread t = new Thread(() -> {
			try {
				mapInterne(m, reader, writer, cb);
			}catch (RemoteException e) {
				e.printStackTrace();
			}
		});

		t.start(); 

		
	}

//string Format reader
	public void mapInterne (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		try {
			
			//redéfinir le reader et le writer pour correspondre à l'architecture d'HDFS

			//Ouverture du reader et du writer
			
			reader.open(OpenMode.R);
			writer.open(OpenMode.W);
			
			//Appel de la fonction map
			m.map(reader, writer);
			
			//Fermeture du reader et du writer
			reader.close();
			writer.close();
			
			//appel du callback à la fin de l'exécution
			cb.MapFinished();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public static void main(String args[]) {  //argument = indice correspondant au nom du pc
		try {
			//creation du serveur de nom
			Registry registry = LocateRegistry.createRegistry(4000);

			//enregistrement auprès du serveur de nom
				//possibilité d'amélioration : récuperer le nom de l'host puis l'écrire dans l'objet projet
			Naming.rebind("//"+Project.nomDeamon[Integer.parseInt(args[0])]+":4000/Daemon", new DaemonImpl());  
			System.out.println("Daemon bound in registry");


			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
