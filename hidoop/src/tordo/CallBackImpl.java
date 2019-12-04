package tordo;

public class CallBackImpl implements CallBack {
	
	private int nbServeurs;
	
	public CallBackImpl(int n) {
		this.nbServeurs = n;
	}

	@Override
	public void MapFinished() throws RemoteException {
		nbServeurs--;
		if (nbServeurs == 0) {
			//Reveiller le thread principal (--> lancer le reduce)
		}
		
	}

}
