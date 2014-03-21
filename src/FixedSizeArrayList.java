import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;

public class FixedSizeArrayList<E> extends CopyOnWriteArrayList<E> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int capacity;
	
	public FixedSizeArrayList(int capacity) {
		super();
		this.capacity = capacity;
	}
	
	@Override
	public synchronized boolean add(E element) {
		boolean result = false;
		if(this.size() < capacity)
			result = true;
		
		super.add(element);
		return result;
	}
	
	/**
	 * Little modified version of default add(index, element) method
	 * 
	 * @param element
	 * @param index
	 * @return
	 */
	public synchronized boolean addAtIndex(int index, E element) {
		boolean result = false;
		if(this.size() < capacity)
			result = true;
		
		super.add(index, element);
		return result;
	}
	
	public boolean isFull() {
		return (this.size() >= capacity);
	}
}
