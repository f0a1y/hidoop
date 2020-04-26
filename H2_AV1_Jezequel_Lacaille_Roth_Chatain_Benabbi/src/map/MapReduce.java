package map;

import java.io.File;
import java.nio.file.Paths;

import config.GeneralConfig;
import hdfs.FileDescriptionI;

public interface MapReduce extends Mapper, Reducer {

    static FileDescriptionI getFile(String parameter) {
    	FileDescriptionI description = null;
    	String[] fileGroup = parameter.trim()
    								  .replaceAll(" -a ", "\0\1")
    								  .replaceAll(" -d ", "\0\2")	
    								  .split(" ");
		for (String fileLabel : fileGroup) {
			String fileName, filePath, fileAlias, fileNameDestination;
			fileAlias = fileNameDestination = null;
			if (!fileLabel.startsWith("\0")) {
				String[] splits = fileLabel.split("\0");
				fileName = splits[0];
				File file = new File(fileName);
				if (file.exists()) {
					fileName = file.getName();
					filePath = file.getAbsolutePath();
				} else 
					filePath = Paths.get(".").toAbsolutePath().normalize().toString();
				for (int j = 1; j < splits.length; j++) {
					switch (splits[j].charAt(0)) {
					case '\1':
						fileAlias = splits[j].substring(1);
						break;
					case '\2':
						fileNameDestination = splits[j].substring(1);
						break;
					}
				}
				description = GeneralConfig.getFileDescription(fileName, filePath, fileAlias, fileNameDestination); 
			}	
		}
		return description;
    }
	
}
