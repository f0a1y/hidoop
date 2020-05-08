package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {
	
	private static final long serialVersionUID = 1L;
	private int numberMaps;
	private SynchronizedList<Integer> channel;
	
	public CallBackImpl(int numberMaps, SynchronizedList<Integer> channel) throws RemoteException {
		this.numberMaps = numberMaps;
		this.channel = channel;
		this.channel.beginInput();
	}

	@Override
	public void dataSent(int daemon) throws RemoteException {
		this.channel.add(daemon);
	}
	
	@Override
	public void MapFinished() throws RemoteException {
		this.numberMaps--;
		if (this.numberMaps == 0)
			this.channel.endInput();
	}

}
