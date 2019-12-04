package tordo;

import config.Project;
import formats.Format.Type;
import hdfs.HdfsClient;
import map.MapReduce;

public class Job implements JobInterface {

		//Format d'entrée
		private Format.Type inputFormat;
	
		//Nom fichier d'entrée
		private String inputFname;

			//Constructeur
	public Job() {
		this.inputFormat = Format.Type.LINE;
		this.inputFname = "../data/filesample.txt";
		this.nbReduce = 1; //On choisit comme valeur par défaut 1 reduce
		this.nbMap = Project.NB_SERVERS; 
		this.outputFormat = Format.Type.KV;
		this.outputFname = "../data/filesample-res.txt";
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



			
			//Creer callbacks cb; 
			CallBack cb = new CallBackImpl(Project.nbMachine);

			// recupération des stubs sur les machines des clusters , voir pour faire directement en fonction de Projet.nbMachine
			Deamon e1 = (Deamon)Naming.lookup("//"+Project.nomDeamon[0]+":4000/Daemon");
			Deamon e2 = (Deamon)Naming.lookup("//"+Project.nomDeamon[1]+":4000/Daemon");

			//recueprer les fragments ecrits sur les machines
			Format reader = formatCreation(this.inputFname+i,this.inputFormat)
			Format writer = new KVFormat( this.inputFname+"-res");


			// E1.runmap(mr,  ,  , cb)
			
			// EN.runmap(mr,  ,  , cb)

			//mettre en veille jusqu'au réveil du callback
			//lancer le reduce

			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


}
