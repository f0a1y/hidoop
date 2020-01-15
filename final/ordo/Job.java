package ordo;

// pour compiler : se placer dans build puis javac ~/2A/hidoop/git/hidoop/hidoop/src/*/*.java

import config.Project;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.OpenMode;
import formats.Format.Type;
import hdfs.HdfsClient;
import map.MapReduce;

import java.rmi.registry.*;
import java.rmi.*;

public class Job implements JobInterface {

		//Format d'entrée
		private Format.Type inputFormat;
	
		//Nom fichier d'entrée
		private String inputFname;

			//Constructeur
	public Job() {
		//valleurs pour l'exemple du sujet
		this.inputFormat = Format.Type.LINE;  
		this.inputFname = "../data/filesample.txt";

		//this.outputFormat = Format.Type.KV;
		//this.outputFname = "../data/filesample-res.txt";
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
			System.out.println(" Job1");
			//objet permettant au callback de communiquer avec le job
			Object temoin = new Object();
			System.out.println(" Job2");
			//Creer callbacks cb; 
			CallBack cb = new CallBackImpl(Project.nbMachine, temoin);
			System.out.println(" Job3");
			// recupération des stubs sur les machines des clusters , voir pour faire directement en fonction de Projet.nbMachine
				//nbMachine défini comme Project.nomDeamon.length
				System.out.println(" Job4");
			Daemon stubs[] = new Daemon[Project.nbMachine+1];
			for (int i = 1; i < Project.nbMachine + 1; i++) {	
				System.out.println(" Job4"+i);
				int port =config.Project.numPortHidoop[i];
				System.out.println(" Job4"+i+"1");
				 String machine = new String(config.Project.nomMachine[i]);
				 System.out.println(config.Project.nomMachine[i]);
				 System.out.println(" Job4"+i+"2");
				 /*
				//Récupération du registre
				String URL = "//"+machine+":"+port;  
				System.out.println(URL);
				Registry reg = (Registry) LocateRegistry.getRegistry(URL);
				System.out.println(" Job4"+i+"3");
				//Récupération du daemon
				Daemon daemon = (Daemon) reg.lookup(URL+"/Daemon");
				System.out.println(" Job4"+i+"4");
				*/
				stubs[i] = (Daemon) Naming.lookup("//"+Project.nomMachine[i]+":"+Project.numPortHidoop[i]+"/Daemon");
			}
				


			System.out.println(" Job5");
			// Ei.runmap(mr,  ,  , cb)
			for (int i = 1; i < Project.nbMachine + 1; i++) {
				System.out.println(" switch 0");
				switch(inputFormat)	{
					case KV : 
					System.out.println(" switch 1");
					stubs[i].runMap(mr, new KVFormat(this.inputFname), new KVFormat(this.inputFname+"-resTemp"), cb);
					break;

					case LINE : 
					System.out.println(" switch 2");
					stubs[i].runMap(mr, new LineFormat(this.inputFname), new KVFormat(this.inputFname+"-resTemp"), cb);
					break;

					default :
					System.out.println(" probleme de format dans le startJob");
				}
			
			}
			System.out.println(" Job6");

			//mettre en veille jusqu'au réveil du callback
			synchronized (temoin) {temoin.wait();}
			System.out.println(" Job7");

			//lecture des résultats avec hdfs
			//hdfs.ServerHDFS.recupererResutats(this.inputFname, "-resTemp");

			System.out.println(" Job8");
			//lancer le reduce
			Format readerReduce = new KVFormat(this.inputFname+"-resTemp"); //ça me semble très faux, on verra plus tard ce qu'il faut mettre
			Format writerReduce = new KVFormat(this.inputFname+"-res");
			System.out.println(" Job9");
			readerReduce.open(OpenMode.R);
			writerReduce.open(OpenMode.W);
			System.out.println(" Job10");
			mr.reduce(readerReduce, writerReduce);
			System.out.println(" Job11");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


}
