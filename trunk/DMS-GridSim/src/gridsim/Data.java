package gridsim;

/**
 *
 * @author Rafael Silva
 */
public class Data implements Comparable<Data> {

    private int id;
    private int size;
    private int creationDate;
    private int compareCode;

    public Data(int id, int size, int compareCode) {
        this.id = id;
        this.size = size;
        this.compareCode = compareCode;
    }

    public int getId() {
        return id;
    }

    public int getSize() {
        return size;
    }

    public int getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(int creationDate) {
        this.creationDate = creationDate;
    }

    public int compareTo(Data o) {
        switch(compareCode) {
            case 1: return this.compareByDate(o);
            default: return 0;
        }
    }

    private int compareByDate(Data o) {
        if (o.getCreationDate() < this.creationDate) {
            return 1;
        } else if (o.getCreationDate() > this.creationDate) {
            return -1;
        }
        return 0;
    }
}
