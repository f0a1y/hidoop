package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {
	
	private static final long serialVersionUID = 1L;
	private int numberMaps;
	private int numberMapBegin;
	private Semaphore beginInput;
	private SynchronizedList<Integer> channel;
	
	public CallBackImpl(int numberMaps, Semaphore beginInput, SynchronizedList<Integer> channel) throws RemoteException {
		this.numberMaps = numberMaps;
		this.numberMapBegin = 0;
		this.beginInput = beginInput;
		this.channel = channel;
		this.channel.beginInput();
	}
	
	@Override
	public void MapBegin() throws RemoteException {
		this.numberMapBegin++;
		if (this.numberMapBegin == this.numberMaps)
			this.beginInput.release();
	}

	@Override
	public void dataSent(int daemon) throws RemoteException {
		this.channel.add(daemon);
	}
	
	@Override
	public void MapFinish() throws RemoteException {
		this.numberMaps--;
		if (this.numberMaps == 0)
			this.channel.endInput();
	}

}
