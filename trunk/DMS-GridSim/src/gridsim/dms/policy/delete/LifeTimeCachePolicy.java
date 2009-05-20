package gridsim.dms.policy.delete;

import gridsim.Data;
import gridsim.Job;
import gridsim.dms.SE;
import gridsim.dms.policy.Policy;
import gridsim.dms.util.DataTransfer;

/**
 *
 * @author Rafael Silva
 */
public class LifeTimeCachePolicy extends Policy {

    private SE se;
    private Job job;

    public LifeTimeCachePolicy(SE se, Job job) {
        this.se = se;
        this.job = job;
    }

    @Override
    public int getTotalRunTime(int time) throws Exception {
        int totalRunTime;
        int dataSize = job.getData().getSize();

        Data data = se.getData(job.getData().getId(), time);
        if (data != null) {
            // Data Reuse
            data.increaseCount();
            data.setLastUsage(time);
            totalRunTime = job.getRunTime() + DataTransfer.intranet(dataSize);
        } else {
            data = se.getCachedData(job.getData().getId(), time);
            if (data != null) {
                // Data Caching
                data.increaseCount();
                data.setLastUsage(time);
                data.setLifetime(time + SE.LIFETIME);
                se.uncacheData(data);
                totalRunTime = job.getRunTime() + DataTransfer.intranet(dataSize);
            } else {
                se.cacheData(time, false);
                if (se.getAvailableSpace() > dataSize) {
                    job.getData().setCreationDate(time);
                    job.getData().setLastUsage(time);
                    job.getData().setLifetime(time + SE.LIFETIME);
                    se.store(job.getData());
                    totalRunTime = job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
                } else {
                    totalRunTime = job.getRunTime() + DataTransfer.extranet(dataSize);
                }
            }
        }
        return totalRunTime;
    }
}
