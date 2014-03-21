import java.util.Stack;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BLinkTree<KeyType extends Comparable<KeyType>, ValueType> {
	public static int internalNodeCapacity;
	public static int leafNodeCapacity;

	private int height;
	private BTreeNode root;

	public BLinkTree(int _iNodeCap, int _lNodeCap) {
		internalNodeCapacity = _iNodeCap;
		leafNodeCapacity = _lNodeCap;
		height = 0;

		root = new LeafNode();
	}

	/**
	 * Follow the required child pointer or link pointer according to the key
	 * 
	 * @param key
	 * @param node
	 * @return
	 */
	private BTreeNode scanNode(KeyType key, BTreeNode node, boolean addingNode) {
		if (node instanceof BLinkTree.LeafNode)
			return node;

		if (!addingNode && node.highKey != null
				&& key.compareTo(node.highKey) >= 0)
			return node.linkPointer;

		int index = 0;
		while (index < node.size() && key.compareTo(node.keyAtIndex(index)) > 0)
			index++;

		return node.childAtIndex(index);
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

		while ((tempNode = scanNode(key, current, true)) == current.linkPointer) {
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
		BTreeNode current = root;
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

			BTreeNode lChild = null, rChild = null;
			boolean result = false;

			while (!result) {
				// Check if the key already exists. Duplicates are not allowed.
				if (current.contains(key))
					return false;

				if (current instanceof BLinkTree.LeafNode) {
					result = ((BLinkTree.LeafNode) current).insert(key, value);

				} else {
					result = ((BLinkTree.IndexNode) current).insert(key,
							lChild, rChild);
				}

				if (!result) {
					BTreeNode newNode = current.splitNode();

					lChild = current;
					rChild = newNode;

					if (current instanceof BLinkTree.LeafNode)
						key = lChild.highKey;
					else
						/* Pushing the leftover key after split to the parent */
						key = ((IndexNode) lChild).leftoverKey;

					/* Time to change the root */
					if (nodeStack.empty()) {
						root = new IndexNode();
						((BLinkTree.IndexNode) root)
								.insert(key, lChild, rChild);
						height++;
						result = true;

					} else {
						current = nodeStack.pop();
						current.lockNode();
						current = moveRight(key, current);
						lChild.unlockNode();
					}
				}
			}

			while (!nodeStack.empty()) {
				IndexNode tNode = (IndexNode) nodeStack.pop();
				tNode.highKey = tNode
						.childAtIndex(tNode.childPointers.size() - 1).highKey;
			}

		} finally {
			current.unlockNode();
		}

		return true;
	}

	private void printInorder() {
		printInorder(root);
	}

	private void printInorder(BTreeNode node) {
		if (node instanceof BLinkTree.IndexNode) {
			System.out.print("{");
			printInorder(node.childAtIndex(0));
			for (int i = 0; i < node.size(); i++) {
				System.out.print(node.keyAtIndex(i) + " ");
				printInorder(node.childAtIndex(i + 1));
			}

			System.out.print(node.highKey + "} ");

		} else {
			System.out.print("[");
			for (int i = 0; i < node.size(); i++)
				System.out.print(node.keyAtIndex(i) + " ");
			System.out.print(node.highKey + "] ");
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

		public boolean contains(KeyType key) {
			return keyList.contains(key);
		}

		public BTreeNode childAtIndex(int index) {
			if (this instanceof BLinkTree.IndexNode)
				return ((IndexNode) this).childPointers.get(index);

			throw new UnsupportedOperationException("Expected class IndexNode");
		}

		public KeyType keyAtIndex(int index) {
			return keyList.get(index);
		}

		public int size() {
			return keyList.size();
		}

		public abstract BTreeNode splitNode();
	}

	private class IndexNode extends BTreeNode {
		public FixedSizeArrayList<BTreeNode> childPointers;

		/*
		 * Used while splitting an index node This key is pushed to the parent
		 * node
		 */
		public KeyType leftoverKey;

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

		public BTreeNode splitNode() {
			IndexNode newNode = new IndexNode();

			int half = size() / 2;
			for (int i = half; i < keyList.size(); i++)
				newNode.insert(keyAtIndex(i), childPointers.get(i),
						childPointers.get(i + 1));

			leftoverKey = keyList.get(half - 1);
			keyList.subList(half - 1, keyList.size()).clear();
			childPointers.subList(half, childPointers.size()).clear();
			highKey = childAtIndex(childPointers.size() - 1).highKey;

			newNode.linkPointer = this.linkPointer;
			this.linkPointer = newNode;

			return newNode;
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

		public BTreeNode splitNode() {
			LeafNode newNode = new LeafNode();

			int half = size() / 2;
			for (int i = half; i < keyList.size(); i++)
				newNode.insert(keyAtIndex(i), recordPointers.get(i + 1));

			keyList.subList(half, keyList.size()).clear();
			recordPointers.subList(half + 1, recordPointers.size()).clear();
			highKey = keyList.get(half - 1);

			newNode.linkPointer = this.linkPointer;
			this.linkPointer = newNode;

			return newNode;
		}
	}

	public static void main(String[] args) {
		BLinkTree<Integer, Integer> bTree = new BLinkTree<>(3, 3);
		bTree.add(50, 50);
		bTree.add(60, 60);
		bTree.add(30, 30);
		bTree.add(40, 40);
		bTree.add(70, 70);
		bTree.add(55, 55);
		bTree.add(45, 45);
		bTree.add(53, 53);
		bTree.add(35, 35);
		bTree.add(25, 25);
		bTree.printInorder();
	}
}
