package gridsim.dms.policy.cache;

import gridsim.Job;
import gridsim.dms.SE;
import gridsim.dms.policy.Policy;
import gridsim.dms.util.DataTransfer;
import java.util.Collections;

/**
 *
 * @author Rafael Silva
 */
public class OldestPolicy extends Policy {

    private SE se;
    private Job job;

    public OldestPolicy(SE se, Job job) {
        this.se = se;
        this.job = job;
    }

    @Override
    public int getTotalRunTime(int time) throws Exception {

        int totalRunTime;
        int dataSize = job.getData().getSize();

        if (se.hasData(job.getData().getId())) {
            System.out.println("---------------------- REUSE");
            totalRunTime = job.getRunTime() + DataTransfer.intranet(dataSize);
        } else {
            if (se.getAvailableSpace() < dataSize) {
                System.out.println("-- SE FULL - DELETE - " + time);
                Collections.sort(se.getDatas());
                se.deleteData(dataSize);
            }
            job.getData().setCreationDate(time);
            se.store(job.getData());
            totalRunTime = job.getRunTime() + DataTransfer.extranet(dataSize) + DataTransfer.intranet(dataSize);
        }
        return totalRunTime;
    }
}
