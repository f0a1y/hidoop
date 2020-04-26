package ordo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject ;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import config.ClusterConfig;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KVFormat;
import hdfs.daemon.FragmentDataI;
import map.Mapper;

import java.io.File;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {
	
	private static final long serialVersionUID = 1L;
	private int id;
	
	//constructeur
	public DaemonImpl(int id) throws RemoteException {
		this.id = id;
	}
	
	//methode distante
	@Override
	public void runMap(final Mapper mapper,
					   final Type inputFormat,
					   final String resultRepertory, 
					   final CallBack callback,  
					   final FragmentDataI data) throws RemoteException {
		// cr�er un thread secondaire qui execute de map pendant qu'on redonne la main au programme principal
		@SuppressWarnings("unchecked")
		List<Integer>[] fragments = new List[ClusterConfig.numberMaps];
		int numberMaps = Math.min(data.getNumberFragments(), ClusterConfig.numberMaps);
		Iterator<Integer> iterator = data.iterator();
		for (int i = 0; i < ClusterConfig.numberMaps; i++)
			fragments[i] = new ArrayList<>();
		for (int i = 0; i < data.getNumberFragments(); i++)
			fragments[i % ClusterConfig.numberMaps].add(iterator.next());
		for (int i = 0; i < numberMaps; i++) {
			final List<Integer> listFragments = fragments[i];
			Thread thread = new Thread() {
				public void run() {
					try {
						mapInterne(mapper, inputFormat, resultRepertory, callback, listFragments, data);
					} catch (RemoteException e) {e.printStackTrace();}
				}
			};

			//lancement du thread secondaire
			thread.start(); 
		}
		for (int i = numberMaps; i < ClusterConfig.numberMaps; i++) 
			callback.MapFinished();
		
	}

	public void mapInterne (Mapper mapper, 
							Type inputFormat, 
							String resultRepertory, 
							CallBack callback,
							List<Integer> fragments,
							FragmentDataI data) throws RemoteException {
		try {	
			Iterator<Integer> iterator = fragments.iterator();
			String repertoryName = ClusterConfig.getDataPath() + data.getFragmentsPath() + resultRepertory;
			File repertory = new File(repertoryName);
			repertory.mkdir();
			while (iterator.hasNext()) {

				//numero du fragment � traiter
				Integer fragment = iterator.next();

				//cr�ation du fragment r�sultat
				String fileWrite = repertoryName + ClusterConfig.fragmentToName(fragment);
				Format writer = new KVFormat(fileWrite);

				//appel du fragment � �tudier
				String fileRead = ClusterConfig.getDataPath() + data.getFragmentsPath() + data.getFragmentName(fragment);
				Format reader = ClusterConfig.selector.selectFormat(inputFormat, fileRead);

				//Ouverture du reader et du writer
				reader.open(OpenMode.R);
				writer.open(OpenMode.W);

				//Appel de la fonction map
				mapper.map(reader, writer);
				
				//Fermeture du reader et du writer
				reader.close();
				writer.close();
			}

			//appel du callback � la fin de l'ex�cution
			callback.MapFinished();

		} catch (Exception e) {
			System.out.println("Erreur dans le map interne");
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) {  
		// r�cup�ration de l'id
		int id = Integer.parseInt(args[0]);

		// r�cup�ration du num�ro de port correspondant
		int port = config.ClusterConfig.hidoopPorts[id];

		// r�cup�ration du nom complet de la machine surlequel est lanc� le daemon
		String host = null;
		try {
			host = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) { 
			e.printStackTrace(); 
			System.exit(0);
		}

		// creation du serveur de nom
		System.out.println("Cr�ation du registre");
		try {
			@SuppressWarnings("unused")
			Registry registry = LocateRegistry.createRegistry(port);
		} catch (RemoteException e) {
			System.out.println("Registre deja cr�e");
		}

		//enregistrement aupr�s du serveur de nom
		try{
			Naming.rebind("//" + host + ":" + port + "/Daemon", new DaemonImpl(id));
			System.out.println("Le daemon " + id + " est lanc� sur la machine " + host + ":" + port);
		} catch (Exception e) {
			System.out.println("Probleme pendant l'enregistrement du daemon " + id + " aupr�s du serveur de nom");
			e.printStackTrace();
			System.exit(0);
		}
	}

}
