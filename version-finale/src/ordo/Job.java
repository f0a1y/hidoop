package ordo;

import java.rmi.Naming;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

import config.ClusterConfig;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KV;
import formats.KVFormat;
import hdfs.FileDescriptionI;
import hdfs.daemon.FragmentDataI;
import hdfs.server.ServerLink;
import map.MapReduce;

public class Job implements JobInterface {

	//Format d'entrée
	private Type inputFormat;
	
	//Nom fichier d'entrée
	private FileDescriptionI inputFile;

	//Constructeur
	public Job() {
		this.inputFormat = Format.Type.LINE;  
	}	

	@Override
	public void setInputFormat(Type inputFormat) {
		this.inputFormat = inputFormat;		
	}

	@Override
	public void setInputFile(FileDescriptionI inputFile) {
		this.inputFile = inputFile;
	}

	@Override
	public void startJob(MapReduce treatment) {
		try {

			// recuperer les machines sur lesquelles sont stockÃ© les fragment du fichier
			HashMap<Integer, FragmentDataI> daemonData = ServerLink.getFragmentData(this.inputFile) ;

			// recupération des stubs sur les machines des clusters : 
			//pour l'instant on ouvre toutes les communications, mais il faudrait dans un 2nd temps n'ouvrir que les communications nÃ©cessaires
			Daemon[] daemons = new Daemon[ClusterConfig.numberDaemons];
			Semaphore beginInput = new Semaphore(0);
			SynchronizedList<Integer> daemonChannel = new SynchronizedList<>(new ArrayList<>(), 1000);
			CallBack callback = new CallBackImpl(daemonData.size(), beginInput, daemonChannel);
			for (int i = 0; i < ClusterConfig.numberDaemons; i++) {
				if (daemonData.containsKey(i)) {
					FragmentDataI data = daemonData.get(i);
					int port = ClusterConfig.ports[ClusterConfig.hidoop][i];
					String host = new String(ClusterConfig.hosts[i]);
					daemons[i] = (Daemon) Naming.lookup("//" + host + ":" + port +"/Daemon");
					daemons[i].runMap(treatment, this.inputFormat, callback, data);
				}
			}
int test = 0; int test1 = 1;
			//lecture des rÃ©sultats avec hdfs
			SynchronizedList<KV> serverChannel = new SynchronizedList<>(new ArrayList<>(), 1000);
			ServerLink link = new ServerLink(daemonData.keySet(), daemonChannel, serverChannel);
			beginInput.acquire();
			link.start();
			
			//lancer le reduce 
			Format writer = new KVFormat(ClusterConfig.fileToFileName(this.inputFile) + "-res");
			writer.open(OpenMode.W);
			treatment.reduce(serverChannel, writer);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}


}
