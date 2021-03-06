package ordo;

import java.util.concurrent.Semaphore;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;

public class CallBackImpl_test extends UnicastRemoteObject implements CallBack {
	
	private int nbServeurs;
	private Object temoin;
	private Semaphore s;
	
	public CallBackImpl_test(int n, Object t) throws RemoteException {
		this.nbServeurs = n;
		this.temoin = t;
		this.s = new Semaphore(1);
		
	}

	@Override
	public void MapFinished() throws RemoteException {

		System.out.println(" CallBack1");
		try{
			//permet un accès exclusif à la décrémentation
			s.acquire();
		} catch (Exception e) {
		e.printStackTrace();				
		}


		// décrémentation du nombre de map en cours
		System.out.println(" CallBack2");
		nbServeurs--;
		System.out.println(" CallBack3");


		// Quand tout les map sont terminé, le CallBack réveille le Job
		if (nbServeurs == 0) {
			synchronized (temoin) {temoin.notify(); }	//Reveiller le thread principal 
			System.out.println(" CallBack Reveil");
		}


		try{
			//permet un accès exclusif à la décrémentation
			s.release();				
		} catch (Exception e) {
			e.printStackTrace();				
		}
		
	}

}
