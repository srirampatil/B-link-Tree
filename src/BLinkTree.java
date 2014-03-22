import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BLinkTree<KeyType extends Comparable<KeyType>, ValueType> {
	public static int internalNodeCapacity;
	public static int leafNodeCapacity;

	private int height;
	private AtomicReference<BTreeNode> root;

	public BLinkTree(int _iNodeCap, int _lNodeCap) {
		internalNodeCapacity = _iNodeCap;
		leafNodeCapacity = _lNodeCap;
		height = 0;

		root = new AtomicReference<BTreeNode>(new BLinkTree.LeafNode());
	}

	public void clear() {
		root = new AtomicReference<BTreeNode>(new BLinkTree.LeafNode());
		height = 0;
	}

	/**
	 * Follow the required child pointer or link pointer according to the key
	 * 
	 * @param key
	 * @param node
	 * @return
	 */
	private BTreeNode scanNode(KeyType key, BTreeNode node, boolean addingNode) {
		while (true) {
			if (node.highKey != null && key.compareTo(node.highKey) > 0
					&& node.linkPointer != null)
				return node.linkPointer;
			
			int index = 0;
			while (index < node.keyList.size()
					&& key.compareTo(node.keyList.get(index)) > 0)
				index++;

			if (node instanceof BLinkTree.LeafNode)
				return node;

			try {
				if (index < ((IndexNode) node).childPointers.size())
					return ((IndexNode) node).childPointers.get(index);
				
			} catch (ArrayIndexOutOfBoundsException e) {
				continue;
			}
		}
	}

	/**
	 * Move as far right as you can to follow link pointers if necessary
	 * 
	 * @param key
	 * @param node
	 * @return
	 */
	private BTreeNode moveRight(KeyType key, BTreeNode node) {
		BTreeNode tempNode = null;
		BTreeNode current = node;

		while ((tempNode = scanNode(key, current, false)) == current.linkPointer) {
			tempNode.lockNode();
			current.unlockNode();
			current = tempNode;
		}

		return current;
	}

	/**
	 * Add a new key, value pair in the BLinkTree Return false if key already
	 * exists and true otherwise
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean add(KeyType key, ValueType value) {
		Stack<BTreeNode> nodeStack = new Stack<BTreeNode>();
		BTreeNode current = root.get();
		BTreeNode node = null;

		// Go down to the leaf node
		while (current instanceof BLinkTree.IndexNode) {
			node = current;
			current = scanNode(key, node, true);
			if (current != node.linkPointer)
				nodeStack.push(node);
		}

		// lock the current node
		current.lockNode();
		try {
			current = moveRight(key, current);

			BTreeNode lChild = null, rChild = null, newRoot = null;
			boolean result = false;

			while (!result) {
				// Check if the key already exists. Duplicates are not allowed.
				if (current.keyList.contains(key))
					return false;

				if (current instanceof BLinkTree.LeafNode) {
					result = ((BLinkTree.LeafNode) current).insert(key, value);

				} else {
					result = ((BLinkTree.IndexNode) current).insert(key,
							lChild, rChild);
				}

				if (!result) {
					Split split = current.splitNode();

					lChild = current;
					rChild = split.newNode;

					if (current instanceof BLinkTree.LeafNode)
						key = split.leftoverKey;
					else
						/* Pushing the leftover key after split to the parent */
						key = split.leftoverKey;

					/* Time to change the root */
					if (nodeStack.empty()) {
						newRoot = new IndexNode();
						if (root.compareAndSet(lChild, newRoot)) {
							// ((BLinkTree.IndexNode) root)
							// .insert(key, lChild, rChild);
							height++;
							// result = true;
							newRoot.lockNode();
							current.unlockNode();
							current = newRoot;
						} else
							result = true;

					} else {
						current = nodeStack.pop();
						current.lockNode();
						current = moveRight(key, current);
						lChild.unlockNode();
					}
				}
			}

//			if (newRoot != null)
//				root.set(newRoot);

			/*
			 * while (!nodeStack.empty()) { IndexNode tNode = (IndexNode)
			 * nodeStack.pop(); tNode.lockNode(); tNode.highKey = tNode
			 * .childPointers.get(tNode.childPointers.size() - 1).highKey;
			 * tNode.unlockNode(); }
			 */

		} catch (ArrayIndexOutOfBoundsException e) {
			return add(key, value);
		} finally {
			current.unlockNode();
		}

		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		BTreeNode lChild = root.get();
		while (lChild instanceof BLinkTree.IndexNode)
			lChild = ((IndexNode) lChild).childPointers.get(0);

		builder.append("[");
		while (lChild != null) {
			builder.append(lChild);
			lChild = lChild.linkPointer;
		}

		builder.replace(builder.length() - 2, builder.length(), "");
		builder.append("]");

		return builder.toString();
	}

	private void printInorder() {
		printInorder(root.get());
	}

	private void printInorder(BTreeNode node) {
		if (node instanceof BLinkTree.IndexNode) {
			System.out.print("{");
			printInorder(((IndexNode) node).childPointers.get(0));
			for (int i = 0; i < node.keyList.size(); i++) {
				System.out.print(node.keyList.get(i) + " ");
				printInorder(((IndexNode) node).childPointers.get(i + 1));
			}

			System.out.print(node.highKey + "} ");

		} else {
			System.out.print("[");
			for (int i = 0; i < node.keyList.size(); i++)
				System.out.print(node.keyList.get(i) + " ");
			System.out.print(node.highKey + "] ");
		}
	}

	private class Split {
		public BTreeNode newNode;
		public KeyType leftoverKey;
		
		public Split(BTreeNode node, KeyType key) {
			newNode = node;
			leftoverKey = key;
		}
	}
	
	private abstract class BTreeNode {
		public KeyType highKey;
		public FixedSizeArrayList<KeyType> keyList;
		public BTreeNode linkPointer;

		protected Lock nodeLock = new ReentrantLock();

		public BTreeNode(int capacity) {
			keyList = new FixedSizeArrayList<KeyType>(capacity);
			linkPointer = null;
			highKey = null;
		}

		public void lockNode() {
			nodeLock.lock();
		}

		public void unlockNode() {
			nodeLock.unlock();
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			for (KeyType key : keyList) {
				builder.append(key.toString() + ", ");
			}
			return builder.toString();
		}

		public abstract Split splitNode();
	}

	private class IndexNode extends BTreeNode {
		public FixedSizeArrayList<BTreeNode> childPointers;

		public IndexNode() {
			super(BLinkTree.internalNodeCapacity);
			childPointers = new FixedSizeArrayList<BTreeNode>(
					BLinkTree.internalNodeCapacity + 1);
		}

		public boolean insert(KeyType key, BTreeNode lChild, BTreeNode rChild) {
			int index = 0;
			while (index < keyList.size()
					&& key.compareTo(keyList.get(index)) > 0)
				index++;

			boolean result = keyList.addAtIndex(index, key);

			if (childPointers.size() > index
					&& childPointers.get(index) != lChild)
				childPointers.remove(index);

			childPointers.addAtIndex(index, lChild);

			if (childPointers.size() > (index + 1)
					&& childPointers.get(index + 1) != rChild)
				childPointers.remove(index + 1);

			childPointers.addAtIndex(index + 1, rChild);

			highKey = childPointers.get(childPointers.size() - 1).highKey;

			return result;
		}

		public Split splitNode() {
			IndexNode newNode = new IndexNode();

			int half = keyList.size() / 2;
			for (int i = half; i < keyList.size(); i++)
				newNode.insert(keyList.get(i), childPointers.get(i),
						childPointers.get(i + 1));

			keyList.subList(half - 1, keyList.size()).clear();
			childPointers.subList(half, childPointers.size()).clear();
			highKey = childPointers.get(childPointers.size() - 1).highKey;

			newNode.linkPointer = this.linkPointer;
			this.linkPointer = newNode;

			return new Split(newNode, keyList.get(half - 1));
		}
	}

	private class LeafNode extends BTreeNode {
		public FixedSizeArrayList<ValueType> recordPointers;

		public LeafNode() {
			super(BLinkTree.leafNodeCapacity);
			recordPointers = new FixedSizeArrayList<ValueType>(
					BLinkTree.leafNodeCapacity + 1);
			recordPointers.add(null);
		}

		public boolean insert(KeyType key, ValueType value) {
			int index = 0;
			while (index < keyList.size()
					&& key.compareTo(keyList.get(index)) > 0)
				index++;

			boolean result = keyList.addAtIndex(index, key);
			recordPointers.addAtIndex(index + 1, value);
			highKey = keyList.get(keyList.size() - 1);
			return result;
		}

		public Split splitNode() {
			LeafNode newNode = new LeafNode();

			int half = keyList.size() / 2;
			for (int i = half; i < keyList.size(); i++)
				newNode.insert(keyList.get(i), recordPointers.get(i + 1));

			keyList.subList(half, keyList.size()).clear();
			recordPointers.subList(half + 1, recordPointers.size()).clear();
			highKey = keyList.get(half - 1);

			newNode.linkPointer = this.linkPointer;
			this.linkPointer = newNode;

			return new Split(newNode, this.highKey);
		}
	}

	public static void main(String[] args) {
		final BLinkTree<Integer, Integer> bTree = new BLinkTree<>(4, 4);

		boolean sequence = false;

		// {{[25 30 30] 30 [35 40 40] 40} 40 {[45 50 50] 50 [53 55 55] 55 [60 70
		// 70] 70} 70}

		if (!sequence) {
			while (true) {
				List<Thread> threadsList = new ArrayList<>();
				final Random random = new Random();
				final ConcurrentSkipListSet<Integer> cSet = new ConcurrentSkipListSet<>();

				for (int i = 0; i < 3; i++) {
					Thread t = new Thread(new Runnable() {
						@Override
						public void run() {
							for (int j = 0; j < 5; j++) {
								int randomInt = random.nextInt(10000);
								cSet.add(randomInt);
								bTree.add(randomInt, randomInt);
							}
						}
					});

					threadsList.add(t);
					t.start();
				}

				for (Thread thread : threadsList) {
					try {
						thread.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				System.out.println("Expected: " + cSet.toString() + "\n"
						+ "Actual: " + bTree.toString());

				if (!cSet.toString().equals(bTree.toString()))
					break;

				cSet.clear();
				bTree.clear();
				threadsList.clear();
			}
		} else {
			bTree.add(70, 50);
			bTree.add(69, 60);
			bTree.add(100, 30);
			bTree.add(99, 40);
			bTree.add(24, 70);
			bTree.add(21, 55);
			bTree.add(14, 45);
			bTree.add(29, 53);
			bTree.add(35, 35);
			bTree.printInorder();
		}
	}
}
