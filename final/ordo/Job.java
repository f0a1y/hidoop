package ordo;

// pour compiler : se placer dans build puis javac ~/2A/hidoop/git/hidoop/hidoop/src/*/*.java

import config.Project;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.OpenMode;
import formats.Format.Type;
import hdfs.ClientHDFS;
import map.MapReduce;

import java.rmi.registry.*;
import java.net.InetAddress;
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


			//Creer callbacks cb; 
			CallBack cb = new CallBackImpl(Project.nbMachine, temoin);


			// recupération des stubs sur les machines des clusters
			Daemon stubs[] = new Daemon[Project.nbMachine+1];
			for (int i = 1; i < Project.nbMachine + 1; i++) {

				int port =config.Project.numPortHidoop[i];
				String machine = new String(config.Project.nomMachine[i]);


				System.out.println(config.Project.nomMachine[i]);

				stubs[i] = (Daemon) Naming.lookup("//"+Project.nomMachine[i]+":"+Project.numPortHidoop[i]+"/Daemon");
				
			}
				


			// lancement en parallèle des maps sur les différents daemons
			for (int i = 1; i < Project.nbMachine + 1; i++) {

				switch(inputFormat)	{

					case KV : 
					stubs[i].runMap(mr, new KVFormat(this.inputFname), new KVFormat(this.inputFname+"-resTemp"), cb);
					break;

					case LINE : 
					stubs[i].runMap(mr, new LineFormat(this.inputFname), new KVFormat(this.inputFname+"-resTemp"), cb);
					break;

					default :
					System.out.println(" probleme de format dans le startJob");
				}
			
			}


			//mettre en veille jusqu'au réveil du callback
			synchronized (temoin) {temoin.wait();}


			//lecture des résultats avec hdfs
			Format readerReduce = new KVFormat(this.inputFname+"-resTemp"); 
			hdfs.serveur.ServerHDFS.recupererResultats(this.inputFname+"-resTemp", readerReduce);


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
