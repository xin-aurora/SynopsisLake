package dataStructure.queue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A class for implementing the Priority queue implement based on
 * https://www.happycoders.eu/algorithms/implement-priority-queue-using-heap/
 * https://github.com/SvenWoltmann/java-collections-guide/blob/main/src/main/java/eu/happycoders/collections/queue/OptimizedHeapPriorityQueue.java
 * 
 * @author xin_aurora
 *
 * @param <E>
 */
public class MyPriorityQueue<E extends Comparable<? super E>> {

	private static final int DEFAULT_INITIAL_CAPACITY = 128;
	private static final int ROOT_INDEX = 0;
	private int numberOfElements;

	private Object[] elements;

	// key is the element, value is the pos in elements[]
	private HashMap<Object, Integer> elementPosMap;

	public MyPriorityQueue() {
		this(DEFAULT_INITIAL_CAPACITY);
	}

	public MyPriorityQueue(int capacity) {
		if (capacity < 1) {
			throw new IllegalArgumentException("Capacity must be 1 or higher");
		}

		elements = new Object[capacity];
		elementPosMap = new HashMap<Object, Integer>();
	}

	public void updatePriority(E newElement, boolean down) {

//		System.out.println("in updatePriority, " + newElement);

			int insertIndex = elementPosMap.get(newElement);

			if (down) {
//				System.out.println("shif down");
				// shif down
				int lastElementInsertIndex = insertIndex;
//				while (isGreaterThanAnyChild(lastElement, lastElementInsertIndex)) {
//					moveSmallestChildUpTo(lastElementInsertIndex);
//					lastElementInsertIndex = smallestChildOf(lastElementInsertIndex);
//				}

				int firstElementWithoutChildren = numberOfElements / 2;
				while (lastElementInsertIndex < firstElementWithoutChildren) {
					int smallestChildIndex = smallestChildOf(lastElementInsertIndex);
					E smallestChild = elementAt(smallestChildIndex);
					boolean lastElementGreaterThanSmallestChild = newElement.compareTo(smallestChild) > 0;
					if (!lastElementGreaterThanSmallestChild) {
						break;
					}

					moveSmallestChildUpTo(smallestChildIndex, lastElementInsertIndex);
					lastElementInsertIndex = smallestChildIndex;
				}

				elements[lastElementInsertIndex] = newElement;
				elementPosMap.put(newElement, lastElementInsertIndex);
			} else {
				// shif up
//				System.out.println("shif up");
				while (isNotRoot(insertIndex)) {
					int parentIndex = parentOf(insertIndex);
					if (!isParentGreater(parentIndex, newElement)) {
						break;
					}

					copyParentDownTo(parentIndex, insertIndex);
					insertIndex = parentIndex;
				}

				elements[insertIndex] = newElement;
				elementPosMap.put(newElement, insertIndex);
			}


	}

	public void add(E newElement) {
		if (numberOfElements == elements.length) {
			grow();
		}
		siftUp(newElement);
		numberOfElements++;
	}

	public E poll() {
		// retrieves the header element,
		E result = elementAtHead();
		// removes the last element
		E lastElement = removeLastElement();
		// moves this last element to its new position.
		siftDown(lastElement);
		elementPosMap.remove(result);
		return result;
	}

	public E peek() {
		return elementAtHead();
	}

	public int size() {
		return numberOfElements;
	}

	public boolean isEmpty() {
		return numberOfElements == 0;
	}

	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		return elementPosMap.containsKey(o);
	}

	public void clear() {
		elementPosMap.clear();
		elements = new Object[DEFAULT_INITIAL_CAPACITY];
	}

	public Set<Object> getRest() {
		return elementPosMap.keySet();
	}

	public HashMap<Object, Integer> getMap() {
		return elementPosMap;
	}

	public Object[] getArray() {
		return elements;
	}

	// copies the array into a new, larger array
	private void grow() {
		int newCapacity = elements.length + elements.length / 2;
		elements = Arrays.copyOf(elements, newCapacity);
	}

	private void siftUp(E newElement) {
//		int insertIndex = numberOfElements;
//
//		while (isNotRoot(insertIndex) && isParentGreater(insertIndex, newElement)) {
//			copyParentDownTo(insertIndex);
//			insertIndex = parentOf(insertIndex);
//		}
//
//		elements[insertIndex] = newElement;
		int insertIndex = numberOfElements;

		while (isNotRoot(insertIndex)) {
			int parentIndex = parentOf(insertIndex);
			if (!isParentGreater(parentIndex, newElement)) {
				break;
			}

			copyParentDownTo(parentIndex, insertIndex);
			insertIndex = parentIndex;
		}

		elements[insertIndex] = newElement;
		elementPosMap.put(newElement, insertIndex);
	}

	private boolean isNotRoot(int index) {
		return index != ROOT_INDEX;
	}

	private boolean isParentGreater(int parentIndex, E element) {
		E parent = elementAt(parentIndex);
		return parent.compareTo(element) > 0;
	}

//	private void copyParentDownTo(int insertIndex) {
//		int parentIndex = parentOf(insertIndex);
//		elements[insertIndex] = elements[parentIndex];
//	}

	private void copyParentDownTo(int parentIndex, int insertIndex) {
		elements[insertIndex] = elements[parentIndex];
		elementPosMap.replace(elements[insertIndex], insertIndex);
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

	// siftDown(): ultimately moves this last element to its new position.
	private void siftDown(E lastElement) {
//		int lastElementInsertIndex = ROOT_INDEX;
//		while (isGreaterThanAnyChild(lastElement, lastElementInsertIndex)) {
//			moveSmallestChildUpTo(lastElementInsertIndex);
//			lastElementInsertIndex = smallestChildOf(lastElementInsertIndex);
//		}

		int lastElementInsertIndex = ROOT_INDEX;
		int firstElementWithoutChildren = numberOfElements / 2;
		while (lastElementInsertIndex < firstElementWithoutChildren) {
			int smallestChildIndex = smallestChildOf(lastElementInsertIndex);
			E smallestChild = elementAt(smallestChildIndex);
			boolean lastElementGreaterThanSmallestChild = lastElement.compareTo(smallestChild) > 0;
			if (!lastElementGreaterThanSmallestChild) {
				break;
			}

			moveSmallestChildUpTo(smallestChildIndex, lastElementInsertIndex);
			lastElementInsertIndex = smallestChildIndex;
		}

		elements[lastElementInsertIndex] = lastElement;
		elementPosMap.replace(lastElement, lastElementInsertIndex);
	}

//	private boolean isGreaterThanAnyChild(E element, int parentIndex) {
//		E leftChild = leftChildOf(parentIndex);
//		E rightChild = rightChildOf(parentIndex);
//
//		return leftChild != null && element.compareTo(leftChild) > 0
//				|| rightChild != null && element.compareTo(rightChild) > 0;
//	}
//
//	private E leftChildOf(int parentIndex) {
//		int leftChildIndex = leftChildIndexOf(parentIndex);
//		return exists(leftChildIndex) ? elementAt(leftChildIndex) : null;
//	}
//
//	private int leftChildIndexOf(int parentIndex) {
//		return 2 * parentIndex + 1;
//	}
//
//	private E rightChildOf(int parentIndex) {
//		int rightChildIndex = rightChildIndexOf(parentIndex);
//		return exists(rightChildIndex) ? elementAt(rightChildIndex) : null;
//	}
//
//	private int rightChildIndexOf(int parentIndex) {
//		return 2 * parentIndex + 2;
//	}

	private boolean exists(int index) {
		return index < numberOfElements;
	}

//	private void moveSmallestChildUpTo(int parentIndex) {
//		int smallestChildIndex = smallestChildOf(parentIndex);
//		elements[parentIndex] = elements[smallestChildIndex];
////		Object parent = elements[parentIndex];
//		elementPosMap.replace(elements[parentIndex], parentIndex);
//	}

	private void moveSmallestChildUpTo(int smallestChildIndex, int parentIndex) {
		elements[parentIndex] = elements[smallestChildIndex];
//		Object parent = elements[parentIndex];
		elementPosMap.replace(elements[parentIndex], parentIndex);
	}

	private int smallestChildOf(int index) {
		int leftChildIndex = 2 * index + 1;
		int rightChildIndex = leftChildIndex + 1;

		if (!exists(rightChildIndex)) {
			return leftChildIndex;
		}

		return smallerOf(leftChildIndex, rightChildIndex);
	}
//	private int smallestChildOf(int parentIndex) {
//		int leftChildIndex = leftChildIndexOf(parentIndex);
//		int rightChildIndex = rightChildIndexOf(parentIndex);
//
//		if (!exists(rightChildIndex)) {
//			return leftChildIndex;
//		}
//
//		return smallerOf(leftChildIndex, rightChildIndex);
//	}

	private int smallerOf(int leftChildIndex, int rightChildIndex) {
		E leftChild = elementAt(leftChildIndex);
		E rightChild = elementAt(rightChildIndex);
		return leftChild.compareTo(rightChild) < 0 ? leftChildIndex : rightChildIndex;
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

}
