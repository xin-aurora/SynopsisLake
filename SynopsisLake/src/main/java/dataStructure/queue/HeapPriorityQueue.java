package dataStructure.queue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * implement based on https://www.happycoders.eu/algorithms/implement-priority-queue-using-heap/
 * https://github.com/SvenWoltmann/java-collections-guide/blob/main/src/main/java/eu/happycoders/collections/queue/OptimizedHeapPriorityQueue.java
 * @author xin_aurora
 *
 * @param <E>
 */

public class HeapPriorityQueue<E extends Comparable<? super E>> implements Queue<E> {

	private static final int DEFAULT_INITIAL_CAPACITY = 100;
	private static final int ROOT_INDEX = 0;
	private int numberOfElements;

	private Object[] elements;


	public HeapPriorityQueue() {
		this(DEFAULT_INITIAL_CAPACITY);
	}

	public HeapPriorityQueue(int capacity) {
		if (capacity < 1) {
			throw new IllegalArgumentException("Capacity must be 1 or higher");
		}

		elements = new Object[capacity];
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return numberOfElements;
	}

	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterator<E> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		return elements;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean remove(Object o) {
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

	// first checks if the queue is full. 
	// If it is, it calls the grow() method,
	// which copies the array into a new, larger array
	@Override
	public boolean add(E e) {
		if (numberOfElements == elements.length) {
			grow();
		}
		siftUp(e);
		numberOfElements++;
		return true;
	}
	
	private void grow() {
		int newCapacity = elements.length + elements.length / 2;
		elements = Arrays.copyOf(elements, newCapacity);
	}

	private void siftUp(E newElement) {
		int insertIndex = numberOfElements;

		while (isNotRoot(insertIndex) && isParentGreater(insertIndex, newElement)) {
			copyParentDownTo(insertIndex);
			insertIndex = parentOf(insertIndex);
		}

		elements[insertIndex] = newElement;
	}

	@Override
	public boolean offer(E e) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public E remove() {
		E result = elementAtHead();
		E lastElement = removeLastElement();
		siftDown(lastElement);
		return result;
	}


	// retrieves the header element,
	// removes the last element, 
	// and then calls siftDown(), 
	// which ultimately moves this last element to its new position.
	@Override
	public E poll() {
		E result = elementAtHead();
		E lastElement = removeLastElement();
		siftDown(lastElement);
		return result;
	}

	@Override
	public E element() {
		// TODO Auto-generated method stub
		return null;
	}

	private boolean isNotRoot(int index) {
		return index != ROOT_INDEX;
	}

	private boolean isParentGreater(int insertIndex, E element) {
		int parentIndex = parentOf(insertIndex);
		E parent = elementAt(parentIndex);
		return parent.compareTo(element) > 0;
	}

	private void copyParentDownTo(int insertIndex) {
		int parentIndex = parentOf(insertIndex);
		elements[insertIndex] = elements[parentIndex];
	}

	private int parentOf(int index) {
		return (index - 1) / 2;
	}

	private E removeLastElement() {
		numberOfElements--;
		E lastElement = elementAt(numberOfElements);
		elements[numberOfElements] = null;
		return lastElement;
	}

	private void siftDown(E lastElement) {
		int lastElementInsertIndex = ROOT_INDEX;
		while (isGreaterThanAnyChild(lastElement, lastElementInsertIndex)) {
			moveSmallestChildUpTo(lastElementInsertIndex);
			lastElementInsertIndex = smallestChildOf(lastElementInsertIndex);
		}

		elements[lastElementInsertIndex] = lastElement;
	}

	private boolean isGreaterThanAnyChild(E element, int parentIndex) {
		E leftChild = leftChildOf(parentIndex);
		E rightChild = rightChildOf(parentIndex);

		return leftChild != null && element.compareTo(leftChild) > 0
				|| rightChild != null && element.compareTo(rightChild) > 0;
	}

	private E leftChildOf(int parentIndex) {
		int leftChildIndex = leftChildIndexOf(parentIndex);
		return exists(leftChildIndex) ? elementAt(leftChildIndex) : null;
	}

	private int leftChildIndexOf(int parentIndex) {
		return 2 * parentIndex + 1;
	}

	private E rightChildOf(int parentIndex) {
		int rightChildIndex = rightChildIndexOf(parentIndex);
		return exists(rightChildIndex) ? elementAt(rightChildIndex) : null;
	}

	private int rightChildIndexOf(int parentIndex) {
		return 2 * parentIndex + 2;
	}

	private boolean exists(int index) {
		return index < numberOfElements;
	}

	private void moveSmallestChildUpTo(int parentIndex) {
		int smallestChildIndex = smallestChildOf(parentIndex);
		elements[parentIndex] = elements[smallestChildIndex];
	}

	private int smallestChildOf(int parentIndex) {
		int leftChildIndex = leftChildIndexOf(parentIndex);
		int rightChildIndex = rightChildIndexOf(parentIndex);

		if (!exists(rightChildIndex)) {
			return leftChildIndex;
		}

		return smallerOf(leftChildIndex, rightChildIndex);
	}

	private int smallerOf(int leftChildIndex, int rightChildIndex) {
		E leftChild = elementAt(leftChildIndex);
		E rightChild = elementAt(rightChildIndex);
		return leftChild.compareTo(rightChild) < 0 ? leftChildIndex : rightChildIndex;
	}

	@Override
	public E peek() {
		return elementAtHead();
	}

	private E elementAtHead() {
		E element = elementAt(0);
		if (element == null) {
			throw new NoSuchElementException();
		}
		return element;
	}

	private E elementAt(int child) {
		@SuppressWarnings("unchecked")
		E element = (E) elements[child];
		return element;
	}

	@Override
	public boolean isEmpty() {
		return numberOfElements == 0;
	}

}
