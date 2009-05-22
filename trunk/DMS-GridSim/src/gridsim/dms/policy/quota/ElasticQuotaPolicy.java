package gridsim.dms.policy.quota;

import gridsim.Data;
import gridsim.Job;
import gridsim.dms.SE;
import gridsim.dms.policy.Policy;
import gridsim.dms.util.DataTransfer;
import java.util.List;

/**
 *
 * @author Rafael Silva
 */
public class ElasticQuotaPolicy extends Policy {

    private SE localSE;
    private List<SE> seList;
    private Job job;

    public ElasticQuotaPolicy(SE localSE, List<SE> seList, Job job) {
        this.localSE = localSE;
        this.seList = seList;
        this.job = job;
    }

    @Override
    public int getTotalRunTime(int time) throws Exception {

        Data jobData = job.getData();
        int dataSize = jobData.getSize();
        int userId = jobData.getUserId();

        // Data Reuse in local SE
        Data data = localSE.getData(jobData.getId(), time);
        if (data != null) {
            data.increaseCount();
            data.setLastUsage(time);
            return job.getRunTime() + DataTransfer.intranet(dataSize);
        }

        // Data Elastic Reuse
        data = localSE.getElasticData(jobData.getId(), time);
        if (data != null) {
            data.increaseCount();
            data.setLastUsage(time);
            return job.getRunTime() + DataTransfer.intranet(dataSize);
        }

        // Data Caching Reuse
        data = localSE.getCachedData(jobData.getId(), time);
        if (data != null && localSE.uncacheElasticData(data, true)) {
            data.increaseCount();
            data.setLastUsage(time);
            data.setLifetime(time + SE.LIFETIME);
            return job.getRunTime() + DataTransfer.intranet(dataSize);
        }

        localSE.cacheData(time, true);
        for (SE se : seList) {
            if (se.getData(jobData.getId(), time) != null) {
                // Copy from another SE
                if (localSE.getQuota(userId) >= dataSize) {
                    storeData(jobData, time, localSE);
                    return job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
                }
                if (localSE.getAvailableElasticSpace() >= dataSize) {
                    storeElasticData(jobData, time);
                    return job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
                }
                // Download from a remote SE to worker
                return job.getRunTime() + DataTransfer.extranet(dataSize);
            }
        }

        // Store in local SE
        if (localSE.getQuota(userId) >= dataSize) {
            storeData(jobData, time, localSE);
            return job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
        }

        localSE.cacheElasticData(time);
        // Elastic Quota
        if (localSE.getAvailableElasticSpace() >= dataSize) {
            storeElasticData(jobData, time);
            return job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
        }

        // Store in a remote SE
        for (SE se : seList) {
            se.cacheData(time, true);
            data = se.getCachedData(jobData.getId(), time);
            if (data != null && se.uncacheData(data, true)) {
                data.increaseCount();
                data.setLastUsage(time);
                data.setLifetime(time + SE.LIFETIME);
                return job.getRunTime() + DataTransfer.extranet(dataSize);
            }
            if (se.getQuota(userId) >= dataSize) {
                storeData(jobData, time, localSE);
                //TODO: Check connection between SEs
                return job.getRunTime() + 2 * DataTransfer.extranet(dataSize);
            }
        }
        return job.getRunTime() + DataTransfer.extranet(dataSize);
    }

    private void storeElasticData(Data data, int time) {
        data.setCreationDate(time);
        data.setLastUsage(time);
        data.setLifetime(time + SE.LIFETIME);
        localSE.storeElastic(data);
    }

    private void storeData(Data data, int time, SE se) {
        data.setCreationDate(time);
        data.setLastUsage(time);
        data.setLifetime(time + SE.LIFETIME);
        se.store(data);
        se.decreaseQuota(data.getUserId(), data.getSize());
    }
}
