package top.wxx.bs.TreeStructure;

import java.util.*;

public class DepthFirstTreeIterator implements Iterator<Node> {
    private LinkedList<Node> list;

    public DepthFirstTreeIterator(HashMap<String, Node> tree, String data) {
        list = new LinkedList<Node>();

        if (tree.containsKey(data)) {
            this.buildList(tree, data);
        }
    }

    private void buildList(HashMap<String, Node> tree, String data) {
        list.add(tree.get(data));
        ArrayList<String> children = tree.get(data).getChildren();
        for (String child : children) {

            // Recursive call
            this.buildList(tree, child);
        }
    }

    @Override
    public boolean hasNext() {
        return !list.isEmpty();
    }

    @Override
    public Node next() {
        return list.poll();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
