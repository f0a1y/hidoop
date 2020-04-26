package ordo;

import java.util.concurrent.Semaphore;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {
	
	private static final long serialVersionUID = 1L;
	private int numberMaps;
	private Object observer;
	private Semaphore gate;
	
	public CallBackImpl(int numberMaps, Object observer) throws RemoteException {
		this.observer = observer;
		this.gate = new Semaphore(1);
		this.numberMaps = numberMaps;
	}
	
	public void addNumberDaemons(int daemons) {
		try {
			this.gate.acquire();
		} catch (InterruptedException e) {e.printStackTrace();}
		this.numberMaps += daemons;
		try {
			this.gate.release();				
		} catch (Exception e) {e.printStackTrace();}
	}

	@Override
	public void MapFinished() throws RemoteException {
		try{
			//permet un accès exclusif à la décrémentation
			this.gate.acquire();
		} catch (InterruptedException e) {e.printStackTrace();}

		// décrémentation du nombre de map en cours
		this.numberMaps--;

		// Quand tout les map sont terminé, le CallBack réveille le Job
		if (this.numberMaps == 0) {
			synchronized (this.observer) {
				this.observer.notify(); 
			}
		}

		try {
			this.gate.release();				
		} catch (Exception e) {e.printStackTrace();}
	}

}
