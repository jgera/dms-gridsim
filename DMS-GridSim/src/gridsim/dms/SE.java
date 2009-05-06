package gridsim.dms;

import gridsim.Data;
import java.util.List;
import java.util.Vector;

/**
 *
 * @author Rafael Silva
 */
public class SE {

    private int size; // in MB
    List<Data> datas;
    private int usedSpace; // in MB

    public SE(int size) {
        this.size = size;
        this.usedSpace = 0;
        this.datas = new Vector<Data>();
    }

    public int getAvailableSpace() {
        return size - usedSpace;
    }

    public List<Data> getDatas() {
        return datas;
    }

    public void deleteData(int requestedSpace) {
        List<Data> toDelete = new Vector<Data>();
        int missingSpace = requestedSpace - (this.size - this.usedSpace);
        int spaceToDelete = 0;

        for (Data data : datas) {
            toDelete.add(data);
            spaceToDelete += data.getSize();

            if (spaceToDelete >= missingSpace) {
                break;
            }
        }

        for (Data data : toDelete) {
            datas.remove(data);
            System.out.println("-- DELETED: " + data.getSize() + " - " + data.getCreationDate());
        }
    }

    public boolean hasData(int dataId) {
        for (Data data : datas) {
            if (data.getId() == dataId) {
                return true;
            }
        }
        return false;
    }

    public void store(Data data) {
        datas.add(data);
        usedSpace += data.getSize();
    }
}
