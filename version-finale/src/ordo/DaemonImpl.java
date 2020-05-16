package ordo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject ;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import config.ClusterConfig;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KV;
import hdfs.daemon.DaemonLink;
import hdfs.daemon.FragmentDataI;
import map.Mapper;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {
	
	private static final long serialVersionUID = 1L;
	private int id;
	
	//constructeur
	public DaemonImpl(int id) throws RemoteException {
		this.id = id;
	}
	
	//methode distante
	@Override
	public void runMap(Mapper mapper,
					   Type inputFormat,
					   CallBack callback,  
					   FragmentDataI data) throws RemoteException {
		Thread thread = new Thread() {
			public void run() {
				try {
					beginMap(mapper, inputFormat, callback, data);
				} catch (RemoteException e) {e.printStackTrace();}
			}
		};
		//lancement du thread secondaire
		thread.start(); 
	}

	public void beginMap (Mapper mapper,
						  Type inputFormat,
						  CallBack callback,  
						  FragmentDataI data) throws RemoteException {
		// créer un thread secondaire qui execute de map pendant qu'on redonne la main au programme principal
		SynchronizedList<KV> hidoopChannel = new SynchronizedList<>(new ArrayList<>(), 1000);
		Map<Integer, List<Integer>> fragments = new HashMap<>();
		int numberMaps;
		if (data != null) {
			numberMaps = Math.min(data.getNumberFragments(), ClusterConfig.numberMaps);
			Iterator<Integer> iterator = data.iterator();
			for (int i = 0; i < ClusterConfig.numberMaps; i++)
				fragments.put(i, new ArrayList<>());
			for (int i = 0; i < data.getNumberFragments(); i++)
				fragments.get(i % ClusterConfig.numberMaps).add(iterator.next());
		} else 
			numberMaps = ClusterConfig.numberMaps;
		for (int i = 0; i < numberMaps; i++) {
			hidoopChannel.beginInput();
			Thread thread;
			if (data != null) {
				final List<Integer> listFragments = fragments.get(i);
				thread = new Thread() {
					public void run() {
						try {
							mapInterne(mapper, hidoopChannel, inputFormat, listFragments, data);
							hidoopChannel.endInput();
						} catch (RemoteException e) {e.printStackTrace();}
					}
				};
			} else {
				thread = new Thread() {
					public void run() {
						try {
							mapInterne(mapper, hidoopChannel);
							hidoopChannel.endInput();
						} catch (RemoteException e) {e.printStackTrace();}
					}
				};
			}
			//lancement du thread secondaire
			thread.start(); 
		}
		
		SynchronizedList<Integer> hdfsChannel = new SynchronizedList<>(new ArrayList<>(), 1000);
		Semaphore beginInput = new Semaphore(0);
		DaemonLink link = new DaemonLink(this.id, beginInput, hidoopChannel, hdfsChannel);
		link.start();
		
		Thread thread = new Thread() {
			public void run() {
				try {
					beginInput.acquire();
					callback.MapBegin();
					List<Integer> daemons = new ArrayList<>();
    	    		while (hdfsChannel.waitUntilIsNotEmpty()) {
			    		hdfsChannel.removeAllInto(100, daemons);
				    	for (Integer daemon : daemons) 
				    		callback.dataSent(daemon);
				    	daemons.clear();
					}
					callback.MapFinish();
				} catch (RemoteException | InterruptedException e) {e.printStackTrace();}
			}
		};
		//lancement du thread secondaire
		thread.start(); 
	}
	
	public void mapInterne(Mapper mapper, 
						   SynchronizedList<KV> hidoopInput,
						   Type inputFormat, 
						   List<Integer> fragments,
						   FragmentDataI data) throws RemoteException {
		try {	
			Iterator<Integer> iterator = fragments.iterator();
			while (iterator.hasNext()) {

				//numero du fragment à traiter
				Integer fragment = iterator.next();

				//appel du fragment à étudier
				String fileRead = data.getFragmentsPath() + data.getFragmentName(fragment);
				Format reader = ClusterConfig.selector.selectFormat(inputFormat, fileRead);

				//Ouverture du reader et du writer
				reader.open(OpenMode.R);

				//Appel de la fonction map
				mapper.map(reader, hidoopInput, this.id);
				
				//Fermeture du reader et du writer
				reader.close();
			}
		} catch (Exception e) {
			System.out.println("Erreur dans le map interne");
			e.printStackTrace();
		}
	}
	
	public void mapInterne(Mapper mapper, SynchronizedList<KV> hidoopInput) throws RemoteException {
		mapper.map(null, hidoopInput, this.id);
	}
	
	public static void main(String args[]) {  
		// récupération de l'id
		int id = Integer.parseInt(args[0]);

		// récupération du numéro de port correspondant
		int port = config.ClusterConfig.ports[ClusterConfig.hidoop][id];

		// récupération du nom complet de la machine surlequel est lancé le daemon
		String host = null;
		try {
			host = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) { 
			e.printStackTrace(); 
			System.exit(0);
		}

		// creation du serveur de nom
		System.out.println("Création du registre");
		try {
			@SuppressWarnings("unused")
			Registry registry = LocateRegistry.createRegistry(port);
		} catch (RemoteException e) {
			System.out.println("Registre deja crée");
		}

		//enregistrement auprès du serveur de nom
		try{
			Naming.rebind("//" + host + ":" + port + "/Daemon", new DaemonImpl(id));
			System.out.println("Le daemon " + id + " est lancé sur la machine " + host + ":" + port);
		} catch (Exception e) {
			System.out.println("Probleme pendant l'enregistrement du daemon " + id + " auprès du serveur de nom");
			e.printStackTrace();
			System.exit(0);
		}
	}

}
