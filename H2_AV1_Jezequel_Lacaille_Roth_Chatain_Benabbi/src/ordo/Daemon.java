package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import formats.Format.Type;
import hdfs.daemon.FragmentDataI;
import map.Mapper;

public interface Daemon extends Remote {
	
	public void runMap (Mapper mapper, 
						Type inputFormat, 
						String resultFormat, 
						CallBack callback, 
						FragmentDataI data) throws RemoteException;

}
