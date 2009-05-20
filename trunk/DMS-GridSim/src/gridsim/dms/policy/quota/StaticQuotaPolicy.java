
package gridsim.dms.policy.quota;

import gridsim.Job;
import gridsim.dms.SE;
import gridsim.dms.policy.Policy;
import java.util.List;

/**
 *
 * @author Rafael Silva
 */
public class StaticQuotaPolicy extends Policy {

    private SE localSE;
    private List<SE> ses;
    private Job job;

    public StaticQuotaPolicy(SE localSE, List<SE> ses, Job job) {
        this.localSE = localSE;
        this.ses = ses;
        this.job = job;
    }
    
    @Override
    public int getTotalRunTime(int time) throws Exception {
        return 0;
    }
}
