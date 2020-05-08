package ordo;

import java.rmi.*;

public interface CallBack extends Remote {
	
	public void dataSent(int daemon) throws RemoteException;
	
	public void MapFinished() throws RemoteException;
	
}
