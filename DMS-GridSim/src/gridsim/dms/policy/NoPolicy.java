package gridsim.dms.policy;

import gridsim.Job;
import gridsim.dms.SE;
import gridsim.dms.util.DataTransfer;

/**
 *
 * @author Rafael Silva
 */
public class NoPolicy extends Policy {

    private SE se;
    private Job job;

    public NoPolicy(SE se, Job job) {
        this.se = se;
        this.job = job;
    }

    @Override
    public int getTotalRunTime(int time) throws Exception {
        int totalRunTime;
        int dataSize = job.getData().getSize();

        if (se.hasData(job.getData().getId())) {
            System.out.println("-- REUSE");
            totalRunTime = job.getRunTime() + DataTransfer.intranet(dataSize);
        } else if (se.getAvailableSpace() >= dataSize) {
            job.getData().setCreationDate(time);
            se.store(job.getData());
            totalRunTime = job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
        } else {
            System.out.println("-- SE FULL");
            totalRunTime = job.getRunTime() + DataTransfer.extranet(dataSize);
        }
        return totalRunTime;
    }
}
