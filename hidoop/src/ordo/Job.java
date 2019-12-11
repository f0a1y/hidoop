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
			//objet permettant au callback de communiquer avec le job
			Object temoin = new Object();
			
			//Creer callbacks cb; 
			CallBack cb = new CallBackImpl(Project.nbMachine, temoin);

			// recupération des stubs sur les machines des clusters , voir pour faire directement en fonction de Projet.nbMachine
				//nbMachine défini comme Project.nomDeamon.length
			Daemon stubs[] = new Daemon[Project.nbMachine];
			for (int i = 0; i < Project.nbMachine; i++) {	
				stubs[i] = (Daemon) Naming.lookup("//"+Project.nomDeamon[i]+":4000/Daemon");
			
			}



			// Ei.runmap(mr,  ,  , cb)
			for (int i = 0; i < Project.nbMachine; i++) {
				switch(inputFormat)	{
					case KV : 
					stubs[i].runMap(mr, new KVFormat(this.inputFname), new KVFormat(this.inputFname+"-resTemp"), cb);

					case LINE : 
					stubs[i].runMap(mr, new LineFormat(this.inputFname), new KVFormat(this.inputFname+"-resTemp"), cb);
				}
			
			}

			//mettre en veille jusqu'au réveil du callback
			temoin.wait();

			//lecture des résultats avec hdfs
				// truc avec des socket, alexandre m'envoie le code


			//lancer le reduce
			Format readerReduce = new KVFormat(this.inputFname+"-resTemp"); //ça me semble très faux, on verra plus tard ce qu'il faut mettre
			Format writerReduce = new KVFormat(this.inputFname+"-res");
			readerReduce.open(OpenMode.R);
			writerReduce.open(OpenMode.R);
			mr.reduce(readerReduce, writerReduce);

			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


}
