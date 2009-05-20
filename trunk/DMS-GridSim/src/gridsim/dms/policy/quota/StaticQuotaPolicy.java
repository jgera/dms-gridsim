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
public class StaticQuotaPolicy extends Policy {

    private SE localSE;
    private List<SE> seList;
    private Job job;

    public StaticQuotaPolicy(SE localSE, List<SE> seList, Job job) {
        this.localSE = localSE;
        this.seList = seList;
        this.job = job;
    }

    @Override
    public int getTotalRunTime(int time) throws Exception {
        int totalRunTime;
        Data jobData = job.getData();
        int dataSize = jobData.getSize();
        int userId = jobData.getUserId();

        Data data = localSE.getData(jobData.getId(), time);
        if (data != null) { // Data Reuse in local SE
            data.increaseCount();
            data.setLastUsage(time);
            totalRunTime = job.getRunTime() + DataTransfer.intranet(dataSize);
        } else {
            localSE.cacheData(time, true);
            if (localSE.getQuota(userId) >= dataSize) {
                for (SE se : seList) {
                    if (se.getData(jobData.getId(), time) != null) {
                        // Copy from another SE
                        jobData.setCreationDate(time);
                        jobData.setLastUsage(time);
                        jobData.setLifetime(time + SE.LIFETIME);
                        localSE.store(jobData);
                        localSE.decreaseQuota(userId, dataSize);
                        totalRunTime = job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
                        break;
                    }
                }
            }


            


            if (localSE.getQuota(userId) >= dataSize) {
                job.getData().setCreationDate(time);
                job.getData().setLastUsage(time);
                job.getData().setLifetime(time + SE.LIFETIME);
                localSE.store(job.getData());
                localSE.decreaseQuota(userId, dataSize);
                totalRunTime = job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
            } else {
                for (SE se : seList) {
                    se.cacheData(time, true);
                    if (se.getQuota(userId) >= dataSize) {
                    }
                }
            }
        }

        return 0; //TODO: totalRunTime
    }
}
