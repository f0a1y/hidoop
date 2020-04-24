package hdfs;

import java.io.Serializable;

public interface FileDescriptionI extends Serializable {

	String getName();
	
	String getPath();
	
	boolean hasAlias();
	
	String getAlias();
	
	boolean hasDestinationName();
	
	String getDestinationName();
	
	void update (FileDescriptionI file);
	
}
