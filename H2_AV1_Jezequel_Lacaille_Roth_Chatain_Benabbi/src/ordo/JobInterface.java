package ordo;

import map.MapReduce;
import formats.Format.Type;
import hdfs.FileDescriptionI;

public interface JobInterface {
	
// Méthodes requises pour la classe Job  
	public void setInputFormat(Type inputFormat);
	
    public void setInputFile(FileDescriptionI inputFile);

    public void startJob (MapReduce mr);
    
}