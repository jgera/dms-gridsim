package gridsim;

/**
 *
 * @author Rafael Silva
 */
public class Data implements Comparable<Data> {

    private int id;
    private int size;
    private int creationDate;
    private int lastUsage;
    private int count;
    private int compareCode;

    public Data(int id, int size, int compareCode) {
        this.id = id;
        this.size = size;
        this.compareCode = compareCode;
        this.count = 0;
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

    public int getCount() {
        return count;
    }

    public void increaseCount() {
        this.count++;
    }

    public void setCreationDate(int creationDate) {
        this.creationDate = creationDate;
    }

    public int getLastUsage() {
        return lastUsage;
    }

    public void setLastUsage(int lastUsage) {
        this.lastUsage = lastUsage;
    }

    public int compareTo(Data o) {
        switch (compareCode) {
            case 1:
                return this.compareByDate(o);
            case 2:
                return this.compareByUsage(o);
            case 3:
                return this.compareByCount(o);
            case 4:
                return this.compareBySize(o);
            default:
                return 0;
        }
    }

    private int compareBySize(Data o) {
        if (o.getSize() < this.size) {
            return 1;
        }
        return -1;
    }

    private int compareByCount(Data o) {
        if (o.getCount() < this.count) {
            return 1;
        }
        return -1;
    }

    private int compareByUsage(Data o) {
        if (o.getLastUsage() < this.lastUsage) {
            return 1;
        }
        return -1;
    }

    private int compareByDate(Data o) {
        if (o.getCreationDate() < this.creationDate) {
            return 1;
        }
        return -1;
    }
}
