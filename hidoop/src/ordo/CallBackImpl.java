package ordo;

import java.util.concurrent.Semaphore;
import java.rmi.*;

public class CallBackImpl implements CallBack {
	
	private int nbServeurs;
	private Object temoin;
	private Semaphore s;
	
	public CallBackImpl(int n, Object t) {
		this.nbServeurs = n;
		this.temoin = t;
		this.s = new Semaphore(1);
	}

	@Override
	public void MapFinished() throws RemoteException {

		try{
			s.acquire();
		} catch (Exception e) {
		e.printStackTrace();				//permet un accès exclusif
		}

		nbServeurs--;
		if (nbServeurs == 0) {
			temoin.notify();	//Reveiller le thread principal 
		}

		try{
			s.release();				//permet un accès exclusif
		} catch (Exception e) {
			e.printStackTrace();				//permet un accès exclusif
		}
		
	}

}
