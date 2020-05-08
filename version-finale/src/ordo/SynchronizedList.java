package ordo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SynchronizedList<E> implements List<E> {

	private int numberInput;
	private int consecutiveModification;
	private int waitModification;
	private int modificationRequired;
	private int numberAccess;
	private int consecutiveAccess;
	private int waitAccess;
	private int limit;
	private List<E> channel;
	private ReentrantLock channelLock;
	private Condition channelInput;
	private Condition channelEmpty;
	private Condition channelNotFull;
	private Condition channelAccessPossible;
	private Condition channelModificationPossible;
	
	public SynchronizedList(List<E> list, int limit) {
		super();
		this.numberInput = 0;
		this.consecutiveModification = 0;
		this.waitModification = 0;
		this.modificationRequired = 0;
		this.numberAccess = 0;
		this.consecutiveAccess = 0;
		this.waitAccess = 0;
		this.limit = limit;
		this.channel = list;
		this.channelLock = new ReentrantLock();
		this.channelInput = this.channelLock.newCondition();
		this.channelEmpty = this.channelLock.newCondition();
		this.channelNotFull = this.channelLock.newCondition();
		this.channelAccessPossible = this.channelLock.newCondition();
		this.channelModificationPossible = this.channelLock.newCondition();
	}
	
	public void beginInput() {
		this.channelLock.lock();
		this.numberInput++;
		this.channelLock.unlock();
	}
	
	public void endInput() {
		this.channelLock.lock();
		this.numberInput--;
		if (this.numberInput == 0) 
			this.channelInput.signalAll();
		this.channelLock.unlock();
	}
	
	public boolean hasInput() {
		this.channelLock.lock();
		boolean result =  this.numberInput > 0;
		this.channelLock.unlock();
		return result;
	}

	public int getNumberInput() {
		this.channelLock.lock();
		int result = this.numberInput;
		this.channelLock.unlock();
		return result;
	}
	
	public boolean waitUntilIsNotEmpty() {
		boolean result = true;
		try {
			this.addAccess();
			if (this.channel.isEmpty())
				result = this.waitUntilInput() || !this.channel.isEmpty();
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}
	
	public boolean add(E element) {
		boolean result = false;
		try {
			boolean signaledAccessEnd = this.modificationLock();
			this.waitUntilNotFull();
			result = this.channel.add(element);
			this.channelInput.signalAll();
			this.modificationUnlock(signaledAccessEnd);
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public void add(int index, E element) {
		if (index < 0 || index >= this.limit)
			throw new IndexOutOfBoundsException();
		else {
			try {
				boolean signaledAccessEnd = this.modificationLock();
				this.waitUntilNotFull();
				this.channel.add(index, element);
				this.channelInput.signalAll();
				this.modificationUnlock(signaledAccessEnd);
			} catch (InterruptedException e) {e.printStackTrace();} 
		}
	}

	public boolean addAll(Collection<? extends E> elements) {
		boolean result = false;
		try {
			boolean signaledAccessEnd = this.modificationLock();
			int numberElements = 0;
			Iterator<? extends E> iterator = elements.iterator();
			while (numberElements < elements.size()) {
				this.waitUntilNotFull();
				int count = 0;
				while (iterator.hasNext() && this.channel.size() < this.limit) {
					result = this.channel.add(iterator.next());
					count++;
				}
				this.channelInput.signalAll();
				numberElements += count;
			}
			this.modificationUnlock(signaledAccessEnd);
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public boolean addAll(int index, Collection<? extends E> elements) {
		if (index < 0 || index + elements.size() >= this.limit)
			throw new IndexOutOfBoundsException();
		else {
			boolean result = false;
			try {
				boolean signaledAccessEnd = this.modificationLock();
				this.waitUntilNotFull();
				result = this.channel.addAll(index, elements);
				if (result)
					this.channelInput.signalAll();
				this.modificationUnlock(signaledAccessEnd);
			} catch (InterruptedException e) {e.printStackTrace();} 
			return result;
		}
	}

	public void clear() {
		try {
			boolean signaledAccessEnd = this.modificationLock();
			this.channel.clear();
			this.channelNotFull.signal();
			this.modificationUnlock(signaledAccessEnd);
		} catch (InterruptedException e) {e.printStackTrace();} 
	}

	public boolean contains(Object element) {
		boolean result = false;
		try {
			this.addAccess();
			result = this.channel.contains(element);
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public boolean waitUntilContains(Object element) {
		boolean result = true;
		try {
			this.addAccess();
			boolean input = true;
			while (input && !this.channel.contains(element))
				input = this.waitUntilInput();
			if (!input)
				result = this.channel.contains(element);
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public boolean containsAll(Collection<?> elements) {
		boolean result = false;
		try {
			this.addAccess();
			result = this.channel.containsAll(elements);
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public boolean waitUntilContainsAll(Collection<?> elements) {
		boolean result = true;
		try {
			this.addAccess();
			boolean input = true;
			while (input && !this.channel.containsAll(elements))
				input = this.waitUntilInput();
			if (!input)
				result = this.channel.containsAll(elements);
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public E get(int index) {
		if (index < 0 || index > this.limit)
			throw new IndexOutOfBoundsException();
		else {
			E result = null;
			try {
				this.addAccess();
				result = this.channel.get(index);
				this.removeAccess();
			} catch (InterruptedException e) {e.printStackTrace();} 
			return result;
		}
	}

	public E waitUntilGet(int index) {
		if (index < 0 || index > this.limit)
			throw new IndexOutOfBoundsException();
		else {
			E result = null;
			try {
				this.addAccess();
				boolean input = true;
				while (input && this.channel.size() <= index)
					input = this.waitUntilInput();
				if (this.channel.size() > index)
					result = this.channel.get(index);
				this.removeAccess();
			} catch (InterruptedException e) {e.printStackTrace();} 
			return result;
		}
	}

	public int indexOf(Object element) {
		int result = -1;
		try {
			this.addAccess();
			result = this.channel.indexOf(element);
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public boolean isEmpty() {
		boolean result = false;
		try {
			this.addAccess();
			result = this.channel.isEmpty();
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public void waitUntilIsEmpty() {
		try {
			this.addAccess();
			if (!this.channel.isEmpty())
				this.waitUntilEmpty();
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
	}

	public Iterator<E> iterator() {
		Iterator<E> result = null;
		try {
			this.addAccess();
			List<E> copy = new ArrayList<>(this.channel);
			result = copy.iterator();
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public int lastIndexOf(Object element) {
		int result = -1;
		try {
			this.addAccess();
			result = this.channel.lastIndexOf(element);
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public ListIterator<E> listIterator() {
		ListIterator<E> result = null;
		try {
			this.addAccess();
			List<E> copy = new ArrayList<>(this.channel);
			result = copy.listIterator();
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public ListIterator<E> listIterator(int index) {
		ListIterator<E> result = null;
		try {
			this.addAccess();
			List<E> copy = new ArrayList<>(this.channel);
			result = copy.listIterator(index);
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public boolean remove(Object element) {
		boolean result = false;
		try {
			boolean signaledAccessEnd = this.modificationLock();
			result = this.channel.remove(element);
			if (result) {
				this.channelNotFull.signal();
				if (this.channel.isEmpty())
					this.channelEmpty.signalAll();
			}
			this.modificationUnlock(signaledAccessEnd);
		} catch (InterruptedException e) {e.printStackTrace();}
		return result;
	}

	public E remove(int index) {
		if (index < 0 || index >= this.limit)
			throw new IndexOutOfBoundsException();
		else {
			E result = null;
			try {
				boolean signaledAccessEnd = this.modificationLock();
				result = this.channel.remove(index);
				this.channelNotFull.signal();
				if (this.channel.isEmpty())
					this.channelEmpty.signalAll();
				this.modificationUnlock(signaledAccessEnd);
			} catch (InterruptedException e) {e.printStackTrace();}
			return result;
		}
	}

	public void removeAllInto(int numberMax, Collection<E> elements) {
		try {
			boolean signaledAccessEnd = this.modificationLock();
			if (this.channel.size() <= numberMax) {
				elements.addAll(this.channel);
				this.channel.clear();
			} else {
				for (int i = 0; i < numberMax; i++)
					elements.add(this.channel.remove(0));
			}
			this.channelNotFull.signal();
			if (this.channel.isEmpty())
				this.channelEmpty.signalAll();
			this.modificationUnlock(signaledAccessEnd);
		} catch (InterruptedException e) {e.printStackTrace();}
	}

	public boolean removeAll(Collection<?> elements) {
		boolean result = false;
		try {
			boolean signaledAccessEnd = this.modificationLock();
			result = this.channel.removeAll(elements);
			if (result) {
				this.channelNotFull.signal();
				if (this.channel.isEmpty())
					this.channelEmpty.signalAll();
			}
			this.modificationUnlock(signaledAccessEnd);
		} catch (InterruptedException e) {e.printStackTrace();}
		return result;
	}

	public boolean retainAll(Collection<?> elements) {
		boolean result = false;
		try {
			boolean signaledAccessEnd = this.modificationLock();
			result = this.channel.retainAll(elements);
			if (result) {
				this.channelNotFull.signal();
				if (this.channel.isEmpty())
					this.channelEmpty.signalAll();
			}
			this.modificationUnlock(signaledAccessEnd);
		} catch (InterruptedException e) {e.printStackTrace();}
		return result;
	}

	public E set(int index, E element) {
		if (index < 0 || index > this.limit)
			throw new IndexOutOfBoundsException();
		else {
			E result = null;
			try {
				boolean signaledAccessEnd = this.modificationLock();
				result = this.channel.set(index, element);
				this.channelInput.signalAll();
				this.modificationUnlock(signaledAccessEnd);
			} catch (InterruptedException e) {e.printStackTrace();} 
			return result;
		}
	}

	public int size() {
		int result = -1;
		try {
			this.addAccess();
			result = this.channel.size();
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public List<E> subList(int fromIndex, int toIndex) {
		List<E> result = null;
		try {
			this.addAccess();
			result = this.channel.subList(fromIndex, toIndex);
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public Object[] toArray() {
		Object[] result = null;
		try {
			this.addAccess();
			result = this.channel.toArray();
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}

	public <T> T[] toArray(T[] array) {
		T[] result = null;
		try {
			this.addAccess();
			result = this.channel.toArray(array);
			this.removeAccess();
		} catch (InterruptedException e) {e.printStackTrace();} 
		return result;
	}
	
	private boolean waitUntilInput() {
		boolean result = false;
		this.channelLock.lock();
		this.modificationRequired++;
		try {
			if (this.hasInput()) {
				if (this.numberAccess == this.modificationRequired)
					this.channelModificationPossible.signal();
				this.channelInput.await();
				if (this.hasInput())
					result = true;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			this.modificationRequired--;
			this.channelLock.unlock();
		}
		return result;
	}
	
	private void waitUntilEmpty() {
		this.channelLock.lock();
		this.modificationRequired++;
		try {
			if (this.numberAccess == this.modificationRequired)
				this.channelModificationPossible.signal();
			this.channelEmpty.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			this.modificationRequired--;
			this.channelLock.unlock();
		}
	}
	
	private void waitUntilNotFull() throws InterruptedException {
		boolean signaled = false;
		while (this.channel.size() >= this.limit) { 
			if (this.numberAccess == this.modificationRequired)
				this.channelModificationPossible.signal();
			this.channelNotFull.await();
			signaled = true;
		}
		if (signaled)
			this.channelNotFull.signal();
	}
	
	private boolean modificationLock() throws InterruptedException {
		this.channelLock.lock();
		boolean signaled = false;
		try {
			while (this.numberAccess > this.modificationRequired || (this.consecutiveModification > 10 && this.waitAccess > 0)) {
				this.waitModification++;
				this.channelModificationPossible.await();
				this.waitModification--;
				signaled = true;
			}
			this.consecutiveModification++;
			this.consecutiveAccess = 0;
		} catch (InterruptedException e) {
			e.printStackTrace();
			this.waitModification--;
			this.channelLock.unlock();
			throw e;
		}
		return signaled;
	}
	
	private void modificationUnlock(boolean signaled) {
		if (signaled) {
			this.channelModificationPossible.signal();
		}
		this.channelAccessPossible.signalAll();
		this.channelLock.unlock();
	}
	
	private void addAccess() throws InterruptedException {
		this.channelLock.lock();
		try {
			while (this.consecutiveAccess > 10 && this.waitModification > 0) {
				this.waitAccess++;
				this.channelAccessPossible.await();
				this.waitAccess--;
			}
			this.numberAccess++;
			this.consecutiveAccess++;
			this.consecutiveModification = 0;
		} catch (InterruptedException e) {
			e.printStackTrace();
			this.waitAccess--;
			throw e;
		} finally {
			this.channelLock.unlock();
		}
	}
	
	private void removeAccess() {
		this.channelLock.lock();
		this.numberAccess--;
		if (this.numberAccess == this.modificationRequired)
			this.channelModificationPossible.signal();
		this.channelLock.unlock();
	}
	
}
