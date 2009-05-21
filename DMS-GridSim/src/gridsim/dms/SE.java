package gridsim.dms;

import gridsim.Data;
import gridsim.scheduler.QuotaScheduler;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

/**
 *
 * @author Rafael Silva
 */
public class SE {

    public static final int LIFETIME = 2000000; // in seconds
    private int size; // in MB
    private List<Data> datas;
    private List<Data> cache;
    private List<Data> elastic;
    private int cacheSize; // in MB
    private int elasticSpace; // in MB
    private int usedSpace; // in MB
    private int[] usersQuota;

    public SE(int size) {
        this.init(size);
    }

    public SE(int size, int quota) {
        this.usersQuota = new int[QuotaScheduler.NUMBER_OF_USERS];
        for (int i = 0; i < QuotaScheduler.NUMBER_OF_USERS; i++) {
            this.usersQuota[i] = quota;
        }
        this.init(size);
    }

    private void init(int size) {
        this.size = size;
        this.usedSpace = 0;
        this.datas = new Vector<Data>();
        this.cache = new Vector<Data>();
        this.cacheSize = 0;
        this.elastic = new Vector<Data>();
        this.elasticSpace = 0;
    }

    public int getAvailableSpace() {
        return size - usedSpace;
    }

    public int getAvailableElasticSpace() {
        return size - (usedSpace + elasticSpace);
    }

    public List<Data> getDatas() {
        return datas;
    }

    public void store(Data data) {
        int availableSpace = size - (usedSpace + cacheSize + elasticSpace);
        if (availableSpace < data.getSize()) {
            int missingSpace = data.getSize() - availableSpace;
            int spaceToDelete = 0;
            List<Data> toDelete = new Vector<Data>();
            // Delete from Cache
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
            // Delete from Elastic Quota
            if (spaceToDelete < missingSpace) {
                Collections.sort(elastic);
                for (Data d : elastic) {
                    spaceToDelete += d.getSize();
                    if (spaceToDelete >= missingSpace) {
                        break;
                    }
                }
                for (Data d : toDelete) {
                    elastic.remove(data);
                    elasticSpace -= d.getSize();
                }
            }
        }
        datas.add(data);
        usedSpace += data.getSize();
    }

    public void storeElastic(Data data) {
        int availableSpace = size - (usedSpace + cacheSize + elasticSpace);
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
        elastic.add(data);
        elasticSpace += data.getSize();
    }

    public void cleanExpiredData(int time) {
        List<Data> toDelete = new Vector<Data>();
        for (Data data : datas) {
            if (data.getLifetime() < time) {
                toDelete.add(data);
            }
        }
        this.deleteData(toDelete, false);
    }

    public void cacheData(int time, boolean updateQuota) {
        List<Data> toCache = new Vector<Data>();
        for (Data data : datas) {
            if (data.getLifetime() < time) {
                toCache.add(data);
                cacheSize += data.getSize();
            }
        }
        cache.addAll(toCache);
        this.deleteData(toCache, updateQuota);
    }

    public void cacheElasticData(int time) {
        List<Data> toCache = new Vector<Data>();
        for (Data data : elastic) {
            if (data.getLifetime() < time) {
                toCache.add(data);
                elasticSpace += data.getSize();
            }
        }
        cache.addAll(toCache);
        this.deleteElasticData(toCache);
    }

    public boolean uncacheData(Data data, boolean updateQuota) {
        if (updateQuota) {
            if (usersQuota[data.getUserId() - 1] > data.getSize()) {
                usersQuota[data.getUserId() - 1] -= data.getSize();
            } else {
                return false;
            }
        }
        cache.remove(data);
        datas.add(data);
        cacheSize -= data.getSize();
        usedSpace += data.getSize();
        return true;
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
        this.deleteData(toDelete, false);
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

    public Data getElasticData(int dataId, int time) {
        for (Data data : elastic) {
            if (data.getId() == dataId) {
                return data;
            }
        }
        return null;
    }

    public int getQuota(int userId) {
        return usersQuota[userId - 1];
    }

    public void decreaseQuota(int userId, int dataSize) {
        usersQuota[userId - 1] -= dataSize;
    }

    public int getElasticSpace() {
        return elasticSpace;
    }

    private void deleteData(List<Data> toDelete, boolean updateQuota) {
        for (Data data : toDelete) {
            datas.remove(data);
            usedSpace -= data.getSize();
            if (updateQuota) {
                usersQuota[data.getUserId() - 1] += data.getSize();
            }
            System.out.println("-- DELETED ID:" + data.getId() + " - SIZE: " + data.getSize() + " - DATE: " + data.getCreationDate() + " - USAGE: " + data.getLastUsage() + " - COUNT: " + data.getCount());
        }
    }

    private void deleteElasticData(List<Data> toDelete) {
        for (Data data : toDelete) {
            elastic.remove(data);
            elasticSpace -= data.getSize();
            System.out.println("-- DELETED ID:" + data.getId() + " - SIZE: " + data.getSize() + " - DATE: " + data.getCreationDate() + " - USAGE: " + data.getLastUsage() + " - COUNT: " + data.getCount());
        }
    }
}
