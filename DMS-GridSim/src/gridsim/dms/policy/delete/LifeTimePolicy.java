package gridsim.dms.policy.delete;

import gridsim.Job;
import gridsim.dms.SE;
import gridsim.dms.policy.Policy;
import gridsim.dms.util.DataTransfer;

/**
 *
 * @author Rafael Silva
 */
public class LifeTimePolicy extends Policy {

    private SE se;
    private Job job;

    public LifeTimePolicy(SE se, Job job) {
        this.se = se;
        this.job = job;
    }

    @Override
    public int getTotalRunTime(int time) throws Exception {
        int totalRunTime;
        int dataSize = job.getData().getSize();

        if (se.hasData(job.getData().getId(), time)) {
            System.out.println("----------- REUSE");
            totalRunTime = job.getRunTime() + DataTransfer.intranet(dataSize);
        } else {
            se.cleanExpiredData(time);
            if (se.getAvailableSpace() > dataSize) {
                totalRunTime = job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
                job.getData().setCreationDate(time);
                job.getData().setLifetime(time + SE.LIFETIME);
                se.store(job.getData());
            } else {
                totalRunTime = job.getRunTime() + DataTransfer.extranet(dataSize);
            }
        }
        return totalRunTime;
    }
}
