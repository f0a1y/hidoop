package tordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import map.Mapper;
import formats.Format;

public interface Daemon extends Remote {
	public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException;
}
