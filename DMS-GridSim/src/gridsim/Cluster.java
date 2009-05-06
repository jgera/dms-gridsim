package gridsim;

import java.util.List;
import java.util.Vector;

/**
 *
 * @author Rafael Silva
 */
public class Cluster {

    private int id;
    private int size;
    private List<Node> nodes;

    public Cluster(int id, int size) {
        this.id = id;
        this.size = size;
        this.nodes = new Vector<Node>(size);

        for (int i = 0; i < size; i++) {
            nodes.add(new Node(i));
        }
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public int getSize() {
        return size;
    }
}
