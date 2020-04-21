package config;

import hdfs.FileDescription;
import hdfs.FileDescriptionI;

public class GeneralConfig {

	// Données de connexion du serveur HDFS
	public final static String host = "localhost";
	public final static int port = 8080;

    public static int BytesInt = Integer.SIZE/Byte.SIZE;	
    public static int BytesLong = Long.SIZE/Byte.SIZE;
    public static int BytesFloat = Float.SIZE/Byte.SIZE;
    public static int BytesDouble = Double.SIZE/Byte.SIZE;

    public static FileDescriptionI getFileDescription(String name, String path, String alias, String destination) {
    	return new FileDescription(name, path, alias, destination);
    }

}
