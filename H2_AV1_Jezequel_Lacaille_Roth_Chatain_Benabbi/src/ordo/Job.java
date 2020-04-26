package ordo;

import java.io.File;
import java.rmi.Naming;
import java.util.HashMap;

import config.ClusterConfig;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KVFormat;
import hdfs.FileDescriptionI;
import hdfs.daemon.FragmentDataI;
import hdfs.server.ServerBridge;
import map.MapReduce;

public class Job implements JobInterface {

	//Format d'entrée
	private Type inputFormat;
	
	//Nom fichier d'entrée
	private FileDescriptionI inputFile;

	//suffixe des dossiers contenant les resultat des maps
	private String resultRepertory = "resTemp" + File.separator;

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

			//objet permettant au callback de communiquer avec le job
			Object observer = new Object();

			// recuperer les machines sur lesquelles sont stockÃ© les fragment du fichier
			HashMap<Integer, FragmentDataI> daemonFragments = ServerBridge.getFragmentData(this.inputFile) ;

			//Creer callback cb : le  

			// recupération des stubs sur les machines des clusters : 
			//pour l'instant on ouvre toutes les communications, mais il faudrait dans un 2nd temps n'ouvrir que les communications nÃ©cessaires
			Daemon daemons[] = new Daemon[ClusterConfig.numberDaemons];
			CallBack callback = new CallBackImpl(daemonFragments.size() * ClusterConfig.numberMaps, observer);
			for (int i = 0; i < ClusterConfig.numberDaemons; i++) {
				if (daemonFragments.containsKey(i)) {
					FragmentDataI data = daemonFragments.get(i);
					int port = ClusterConfig.hidoopPorts[i];
					String host = new String(ClusterConfig.hosts[i]);
					daemons[i] = (Daemon) Naming.lookup("//" + host + ":" + port +"/Daemon");
					daemons[i].runMap(treatment, this.inputFormat, this.resultRepertory, callback, data);
				}
			}

			//mettre en veille jusqu'au rÃ©veil du callback
			synchronized (observer) {
				observer.wait();
			}

			//lecture des rÃ©sultats avec hdfs
			String file = ClusterConfig.fileToFileName(this.inputFile) + "-" + this.resultRepertory;
			Format reader = new KVFormat(file); 
			reader.open(OpenMode.W);
			ServerBridge.writeFragments(this.inputFile, daemonFragments.keySet(), this.resultRepertory, reader);
			reader.close();

			//lancer le reduce 
			Format writer = new KVFormat(ClusterConfig.fileToFileName(this.inputFile) + "-res");
			reader.open(OpenMode.R);
			writer.open(OpenMode.W);
			treatment.reduce(reader, writer);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}


}
