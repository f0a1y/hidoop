package tordo;

import formats.Format.Type;
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
		//Creer callbacks cb; 
		// N E1...EN objet de type DEAMON (objet remote...)
		Deamon e1 = Naming.lookup("//"+Project.nomDeamon[0]+":4000/Daemon");
		Deamon e2 = Naming.lookup("//"+Project.nomDeamon[1]+":4000/Daemon");


		// E1.runmap(mr,  ,  , cb)
		
		// EN.runmap(mr,  ,  , cb)
		// TODO Auto-generated method stub
		
	}

}
