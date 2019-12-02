package tordo;

import java.rmi.*;

public interface CallBack extends Remote {
	
	public void MapFinished() throws RemoteException;
}
