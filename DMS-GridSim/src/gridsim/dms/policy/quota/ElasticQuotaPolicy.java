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

        Data data = localSE.getData(jobData.getId(), time);
        if (data != null) { // Data Reuse in local SE
            data.increaseCount();
            data.setLastUsage(time);
            return job.getRunTime() + DataTransfer.intranet(dataSize);
        } else {
            localSE.cacheData(time, true);
            for (SE se : seList) {
                if (se.getData(jobData.getId(), time) != null) {
                    if (localSE.getQuota(userId) >= dataSize) {
                        // Copy from another SE
                        storeData(jobData, time, localSE);
                        return job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
                    } else {
                        // Download from a remote SE to worker
                        return job.getRunTime() + DataTransfer.extranet(dataSize);
                    }
                }
            }
            if (localSE.getQuota(userId) >= dataSize) {
                // Store in local SE
                storeData(jobData, time, localSE);
                return job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
            } else {
                for (SE se : seList) {
                    se.cacheData(time, true);
                    if (se.getQuota(userId) >= dataSize) {
                        // Store in a remote SE
                        storeData(jobData, time, localSE);
                        //TODO: Check connection between SEs
                        return job.getRunTime() + 2 * DataTransfer.extranet(dataSize);
                    }
                }
                return job.getRunTime() + DataTransfer.extranet(dataSize);
            }
        }
    }

    private void storeData(Data data, int time, SE se) {
        data.setCreationDate(time);
        data.setLastUsage(time);
        data.setLifetime(time + SE.LIFETIME);
        se.store(data);
        se.decreaseQuota(data.getUserId(), data.getSize());
    }
}