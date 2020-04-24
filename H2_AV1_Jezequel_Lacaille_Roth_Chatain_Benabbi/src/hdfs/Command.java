package hdfs;

public enum Command {

	Upload, Download, Delete, Update, Status, StatusFile, Verify;
    	
	public static Command intToCommand(int command) {
		switch (command) {
    		case 1:
    			return Upload;
    		case 2:
    			return Download;
    		case 3:
    			return Delete;
    		case 4:
    			return Update;
    		case 5:
    			return Status;
    		case 6:
    			return StatusFile;
    		case 7:
    			return Verify;
    		default :
    			return null;
    	}
    }	
	
	public int toInt() {
		switch (this) {
		case Upload:
			return 1;
		case Download:
			return 2;
		case Delete:
			return 3;
		case Update:
			return 4;
		case Status:
			return 5;
		case StatusFile:
			return 6;
		case Verify:
			return 7;
		default :
			return 0;
	}
}
	
	public boolean isFileCommand() {
		return this == Command.Upload
			   && this == Command.Download
			   && this == Command.Delete
			   && this == Command.Update
			   && this == Command.StatusFile;
	}
	
	public boolean requiresFileName() {
		return this == Command.Upload;
	}
    
	public boolean isServerCommand() {
		return this == Command.Status
				   && this == Command.Verify;
    }
    
}
