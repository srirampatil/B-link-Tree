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
	private BTreeNode scanNode(KeyType key, BTreeNode node) {
		if (key.compareTo(node.highKey) >= 0)
			return node.linkPointer;

		if (node instanceof BLinkTree.LeafNode)
			return node;

		int index = 0;
		while (key.compareTo(node.keyList.get(index)) > 0)
			index++;

		return ((IndexNode) node).childPointers.get(index);
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

		while ((tempNode = scanNode(key, current)) == current.linkPointer) {
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
			current = scanNode(key, node);
			if (current != node.linkPointer)
				nodeStack.push(node);
		}

		// lock the current node
		current.lockNode();
		try {
			current = moveRight(key, current);

			BTreeNode lChild = null, rChild = null;
			boolean result = false;

			while (!result && !nodeStack.empty()) {
				// Check if the key already exists. Duplicates are not allowed.
				if (current.contains(key))
					return false;

				if (current instanceof BLinkTree.LeafNode) {
					result = ((BLinkTree.LeafNode) current).insert(key, value);

				} else {
					result = ((BLinkTree.IndexNode) current).insert(key, lChild, rChild);
				}

				if (!result) {
					BTreeNode newNode = current.splitNode();

					lChild = current;
					rChild = newNode;
					key = lChild.highKey;

					current = nodeStack.pop();
					current.lockNode();
					current = moveRight(key, current);

					lChild.unlockNode();
				}
			}
			
			/* Changing the root. All nodes got split */
			if(!result && nodeStack.empty()) {
				root = new IndexNode();
				((BLinkTree.IndexNode) current).insert(key, lChild, rChild);
				height++;
			}

		} finally {
			current.unlockNode();
		}

		return true;
	}

	private abstract class BTreeNode {
		public KeyType highKey;
		public FixedSizeArrayList<KeyType> keyList;
		public BTreeNode linkPointer;

		protected Lock nodeLock = new ReentrantLock();

		public BTreeNode(int capacity) {
			keyList = new FixedSizeArrayList<KeyType>(capacity);
			linkPointer = null;
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

		public abstract BTreeNode splitNode();
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
					&& childPointers.get(index) != lChild) {
				childPointers.remove(index);
				childPointers.addAtIndex(index, lChild);
			}

			if (childPointers.size() > (index + 1)
					&& childPointers.get(index + 1) != rChild) {
				childPointers.remove(index + 1);
				childPointers.addAtIndex(index + 1, rChild);
			}

			highKey = keyList.get(keyList.size() - 1);

			return result;
		}

		public BTreeNode splitNode() {
			// TODO: index node splitting
			return null;
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

			int half = keyList.size() / 2;
			for (int i = half; i < keyList.size(); i++)
				newNode.insert(keyList.get(i), recordPointers.get(i + 1));

			keyList.subList(half, keyList.size()).clear();
			recordPointers.subList(half + 1, recordPointers.size()).clear();
			highKey = keyList.get(half - 1);

			newNode.linkPointer = this.linkPointer;
			this.linkPointer = newNode;

			return newNode;
		}
	}
}
