package tordo;

import java.rmi.*;

import map.Mapper;
import formats.Format

public class DaemonImpl extends UnicastRemoteObject implements Daemon {
	
	
	//constructeur
	public DaemonImpl() throws RemoteException {
	
	}
	
	//methode distante
	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {

			// creer un thread secondaire qui execute de map pendant qu'on redonne la main au prgm principal
			Runnable rb = new Runnable(){
				pubic void run() {
					try {
						self.map(m, reader, writer, cb);
					}catch (RemoteException e) {
						e.printStackTrace();
					}
				}
			}



		

		
	}
//string Format reader
	public map (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		try {
			// creer un thread secondaire pour redonner la main au prgm principal

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
	
	public static void main(String args[]) {
		try {
			//creation du serveur de nom
			Registry registry = LocateRegistry.createRegistry(4000);

			//enregistrement auprès du serveur de nom
			Naming.rebind("//"+Project.nomDeamon[args[0]]+":4000/Daemon", new DaemonImpl());
			System.out.println("Daemon bound in registry");


			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
