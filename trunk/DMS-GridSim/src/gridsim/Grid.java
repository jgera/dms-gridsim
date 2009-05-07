package gridsim;

import gridsim.dms.SE;
import gridsim.dms.policy.PolicyType;

/**
 *
 * @author Rafael Silva
 */
public class Grid {

    private Workload wl;

    public static void main(String[] args) {
        try {
            Grid grid = new Grid();
            grid.generateWorkload("/test.db3", 100, 90);
            int SE_SIZE = 3000; // 30%
            
//            grid.process("/test.db3", "/testResult_0.db3", PolicyType.NO_POLICY, 20, SE_SIZE);

            // CACHE POLICIES
//            grid.process("/test.db3", "/testResult_1.db3", PolicyType.OLDEST_CACHE_POLICY, 20, SE_SIZE);
//            grid.process("/test.db3", "/testResult_2.db3", PolicyType.LRU_CACHE_POLICY, 20, SE_SIZE);
//            grid.process("/test.db3", "/testResult_3.db3", PolicyType.MOU_CACHE_POLICY, 20, SE_SIZE);
//            grid.process("/test.db3", "/testResult_4.db3", PolicyType.SIZE_CACHE_POLICY, 20, SE_SIZE);

            // DELETE POLICIES
            grid.process("/test.db3", "/testResult_5.db3", PolicyType.LIFETIME_DELETE_POLICY, 20, SE_SIZE);
            grid.process("/test.db3", "/testResult_6.db3", PolicyType.LIFETIME_INCREASE_DELETE_POLICY, 20, SE_SIZE);
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void process(String workload, String result, int compareCode,
            int numNodes, int seCapacity) throws Exception {

        // Configuring
        wl = new Workload(workload);
        wl.prepareToRead(compareCode);

        Cluster cluster = new Cluster(1, numNodes);
        SE se = new SE(seCapacity);
        Output out = new Output(result);

        Scheduler scheduler = new Scheduler(compareCode, wl, out, cluster, se);
        scheduler.run();
    }

    private void generateWorkload(String workload, int numOfJobs, double reuse) throws Exception {
        wl = new Workload(workload);
        wl.generate(numOfJobs, reuse);
    }
}
