package ordo;

// pour compiler : se placer dans build puis javac ~/2A/hidoop/git/hidoop/hidoop/src/*/*.java

import config.ClusterConfig;
import config.GeneralConfig;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.OpenMode;
import formats.Format.Type;
import hdfs.ClientHDFS;
import hdfs.FileDescriptionI;
import hdfs.daemon.FragmentDataI;
import hdfs.server.ServerBridge;
import hdfs.server.ServerHDFS;
import map.MapReduce;

import java.rmi.registry.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.net.InetAddress;
import java.rmi.*;

public class Job implements JobInterface {

		//Format d'entrée
		private Format.Type inputFormat;
	
		//Nom fichier d'entrée
		private String inputFname;

		//suffixe des dossiers contenant les resultat des maps
		private String suffixeResultat = "resTemp";



	//Constructeur
	public Job() {
		//valleurs pour l'exemple du sujet
		this.inputFormat = Format.Type.LINE;  
		this.inputFname = "";;
	}	


	@Override
	public void setInputFormat(Type ft) {
		this.inputFormat = ft;
		
	}

	@Override
	public void setInputFname(String fname) {
		this.inputFname = fname;
		
	}

	@Override
	public void startJob(MapReduce mr) {
		try {

			//objet permettant au callback de communiquer avec le job
			Object temoin = new Object();

			// recuperer les machines sur lesquelles sont stocké les fragment du fichier
			FileDescriptionI file = GeneralConfig.getFileDescription(this.inputFname, null, null, null);
			HashMap<Integer, FragmentDataI> daemonsConcernes = ServerBridge.getFileData(file) ;
			int nbMachine = daemonsConcernes.size();

			//Creer callback cb : le  
			CallBack cb = new CallBackImpl(nbMachine, temoin);



			// recupération des stubs sur les machines des clusters : 
				//pour l'instant on ouvre toutes les communications, mais il faudrait dans un 2nd temps n'ouvrir que les communications nécessaires
			Daemon stubs[] = new Daemon[ClusterConfig.nbMachine];
			for (int i = 0; i < ClusterConfig.nbMachine; i++) {

				int port = config.ClusterConfig.numPortHidoop[i];
				String machine = new String(config.ClusterConfig.nomMachine[i]);


				System.out.println(config.ClusterConfig.nomMachine[i]);

				stubs[i] = (Daemon) Naming.lookup("//"+ClusterConfig.nomMachine[i]+":"+ClusterConfig.numPortHidoop[i]+"/Daemon");
				
			}
				


			// lancement en parallèle des maps sur les différents daemons

			for (HashMap.Entry<Integer, FragmentDataI> mapentry : daemonsConcernes.entrySet()) {
				Integer i = mapentry.getKey() ;
				List<Integer> numFragment =  new ArrayList<>();
				numFragment.addAll(mapentry.getValue().getFragments());
			 
				stubs[i].runMap(mr, inputFormat, inputFname, suffixeResultat, cb, numFragment);

			
			}


			//mettre en veille jusqu'au réveil du callback
			synchronized (temoin) {temoin.wait();}


			//lecture des résultats avec hdfs
			String emplacement = this.inputFname + suffixeResultat ;
			Format readerReduce = new KVFormat(emplacement); 
			readerReduce.open(OpenMode.W);
			ServerBridge.recupererResultats(file, readerReduce);
			readerReduce.close();

			//lancer le reduce 
			Format writerReduce = new KVFormat(this.inputFname+"-res");

			readerReduce.open(OpenMode.R);
			writerReduce.open(OpenMode.W);

			mr.reduce(readerReduce, writerReduce);
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}


}
