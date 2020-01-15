package ordo;

import java.util.concurrent.Semaphore;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {
	
	private int nbServeurs;
	private Object temoin;
	private Semaphore s;
	
	public CallBackImpl(int n, Object t) throws RemoteException {
		this.nbServeurs = n;
		this.temoin = t;
		this.s = new Semaphore(1);
	}

	@Override
	public void MapFinished() throws RemoteException {

		try{
			System.out.println(" CallBack1");
			s.acquire();
		} catch (Exception e) {
		e.printStackTrace();				//permet un accès exclusif
		}
		System.out.println(" CallBack2");
		nbServeurs--;
		System.out.println(" CallBack3");
		if (nbServeurs == 0) {
			synchronized (temoin) {temoin.notify(); }	//Reveiller le thread principal 
			System.out.println(" CallBack Reveil");
		}

		try{
			s.release();				//permet un accès exclusif
		} catch (Exception e) {
			e.printStackTrace();				//permet un accès exclusif
		}
		
	}

}
