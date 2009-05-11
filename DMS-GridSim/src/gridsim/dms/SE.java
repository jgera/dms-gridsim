package gridsim.dms;

import gridsim.Data;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

/**
 *
 * @author Rafael Silva
 */
public class SE {

    private int size; // in MB
    private List<Data> datas;
    private List<Data> cache;
    private int cacheSize;
    private int usedSpace; // in MB
    public static final int LIFETIME = 1000; // in seconds

    public SE(int size) {
        this.size = size;
        this.usedSpace = 0;
        this.datas = new Vector<Data>();
        this.cache = new Vector<Data>();
        this.cacheSize = 0;
    }

    public int getAvailableSpace() {
        return size - usedSpace;
    }

    public List<Data> getDatas() {
        return datas;
    }

    public void store(Data data) {
        int availableSpace = size - (usedSpace + cacheSize);
        if (availableSpace < data.getSize()) {
            int missingSpace = data.getSize() - availableSpace;
            int spaceToDelete = 0;
            List<Data> toDelete = new Vector<Data>();
            Collections.sort(cache);
            for (Data d : cache) {
                spaceToDelete += d.getSize();
                if (spaceToDelete >= missingSpace) {
                    break;
                }
            }
            for (Data d : toDelete) {
                cache.remove(data);
                cacheSize -= d.getSize();
            }
        }
        datas.add(data);
        usedSpace += data.getSize();
    }

    public void cleanExpiredData(int time) {
        List<Data> toDelete = new Vector<Data>();
        for (Data data : datas) {
            if (data.getLifetime() < time) {
                toDelete.add(data);
            }
        }
        this.deleteData(toDelete);
    }

    public void cacheData(int time) {
        List<Data> toCache = new Vector<Data>();
        for (Data data : datas) {
            if (data.getLifetime() < time) {
                toCache.add(data);
                cacheSize += data.getSize();
            }
        }
        cache.addAll(toCache);
        this.deleteData(toCache);
    }

    public void uncacheData(Data data) {
        cache.remove(data);
        datas.add(data);
        cacheSize -= data.getSize();
        usedSpace += data.getSize();
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
        this.deleteData(toDelete);
    }

    public boolean hasData(int dataId) {
        for (Data data : datas) {
            if (data.getId() == dataId) {
                data.increaseCount();
                return true;
            }
        }
        return false;
    }

    public Data getData(int dataId, int time) {
        for (Data data : datas) {
            if (data.getId() == dataId) {
                return data;
            }
        }
        return null;
    }

    public Data getCachedData(int dataId, int time) {
        for (Data data : cache) {
            if (data.getId() == dataId) {
                return data;
            }
        }
        return null;
    }

    private void deleteData(List<Data> toDelete) {
        for (Data data : toDelete) {
            datas.remove(data);
            usedSpace -= data.getSize();
            System.out.println("-- DELETED ID:" + data.getId() + " - SIZE: " + data.getSize() + " - DATE: " + data.getCreationDate() + " - USAGE: " + data.getLastUsage() + " - COUNT: " + data.getCount());
        }
    }
}
