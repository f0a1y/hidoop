package tordo;

import java.rmi.*;

import map.Mapper;
import formats.Format

public class DaemonImpl implements Daemon {
	
	

	public DaemonImpl() throws RemoteException {
	
	}
	
	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
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
			Registry registry = LocateRegistry.createRegistry(4000);

			Naming.rebind("//"+Project.nomDeamon[args[0]]+":4000/Daemon", new DaemonImpl());
			System.out.println("Daemon bound in registry");


			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
