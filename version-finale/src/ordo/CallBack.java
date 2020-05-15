package ordo;

import java.rmi.*;

public interface CallBack extends Remote {
	
	public void MapBegin() throws RemoteException;
	
	public void dataSent(int daemon) throws RemoteException;
	
	public void MapFinish() throws RemoteException;
	
}
