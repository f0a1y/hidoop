package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import map.Mapper;
import formats.Format;

public interface Daemon extends Remote {
	public void runMap (Mapper m, Format.Type  inputFormat, String inputFname, String suffixeResultat, CallBack cb, List<Integer> numFragment) throws RemoteException;


}
