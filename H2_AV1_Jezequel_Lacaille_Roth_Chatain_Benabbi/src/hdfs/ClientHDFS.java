package hdfs;

import config.ClientConfig;

public class ClientHDFS {

    public static void main(String[] args) {
    	if (args.length == 2) {
            int command = Integer.parseInt(args[0]);
    		String fileName = args[1];
            if (ClientConfig.selector.knowsFileFormat(fileName)) {
		        try {
		            ActivityI activity = ClientConfig.getClientActivity();
		    	   	activity.start(command, fileName);
		        } catch (Exception e) {e.printStackTrace();}
            } else {
            	System.out.println("format du fichier inconnu");
            }
    	} else {
    		System.out.println("Usage : java hdfs.ClientHDFS <command> <fileName>");
    	}
    }
    
}
