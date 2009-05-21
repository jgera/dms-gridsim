package gridsim;

import gridsim.scheduler.Scheduler;
import gridsim.dms.SE;
import gridsim.dms.policy.PolicyType;
import gridsim.scheduler.DataExclusionScheduler;
import gridsim.scheduler.QuotaScheduler;

/**
 *
 * @author Rafael Silva
 */
public class Grid {

    private Workload wl;

    public static void main(String[] args) {
        try {
            Grid grid = new Grid();
            if (args.length == 0) {
                grid.generateWorkload("workload/input/input-100000-10.db3", 100000, 10);
                grid.generateWorkload("workload/input/input-100000-25.db3", 100000, 25);
                grid.generateWorkload("workload/input/input-100000-50.db3", 100000, 50);
                grid.generateWorkload("workload/input/input-100000-75.db3", 100000, 75);
                grid.generateWorkload("workload/input/input-100000-90.db3", 100000, 90);
            } else {
                String input = args[0];
                String output = args[1];
                int policy = new Integer(args[2]);
                int nodes = new Integer(args[3]);
                int seSize = new Integer(args[4]);

                if (args.length == 5) {
                    grid.process(input, output, policy, nodes, seSize);
                } else if (args.length == 6) {
                    int seListSize = new Integer(args[5]);
                    grid.process(input, output, policy, nodes, seSize, seListSize);
                }
            }
//            grid.test(grid);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // Data Exclusion Policy
    private void process(String workload, String result, int compareCode,
            int numNodes, int seCapacity) throws Exception {

        // Configuring
        wl = new Workload(workload);
        wl.prepareToRead(compareCode);

        Cluster cluster = new Cluster(1, numNodes);
        SE se = new SE(seCapacity);
        Output out = new Output(result);

        Scheduler scheduler = new DataExclusionScheduler(compareCode, wl, out, cluster, se);
        scheduler.run();
    }

    // Quota Policy
    private void process(String workload, String result, int compareCode,
            int numNodes, int seCapacity, int seListSize) throws Exception {

        // Configuring
        wl = new Workload(workload);
        wl.prepareToRead(compareCode);

        Cluster cluster = new Cluster(1, numNodes);
        Output out = new Output(result);

        Scheduler scheduler = new QuotaScheduler(compareCode, wl, out, cluster, seListSize, seCapacity);
        scheduler.run();
    }

    private void generateWorkload(String workloadName, int numOfJobs, double reuse) throws Exception {
        wl = new Workload(workloadName);
        wl.generate(numOfJobs, reuse);
    }

    private void test(Grid grid) throws Exception {
        int SE_SIZE = 163840; // in MB

        grid.generateWorkload("/t.db3", 500, 90);
//        grid.process("/t.db3", "/tR1.db3", PolicyType.LIFETIME_CACHE_POLICY, 20, SE_SIZE);
//        grid.process("/t.db3", "/tR2.db3", PolicyType.LIFETIME_CACHE_COUNT_POLICY, 20, SE_SIZE);
//        grid.process("/t.db3", "/tR3.db3", PolicyType.LIFETIME_INCREASE_CACHE_POLICY, 20, SE_SIZE);

        //DELETE POLICIES
//        grid.process("/test.db3", "/testResult_5.db3", PolicyType.LIFETIME_POLICY, 20, SE_SIZE);
//        grid.process("/test.db3", "/testResult_6.db3", PolicyType.LIFETIME_INCREASE_POLICY, 20, SE_SIZE);
//        grid.process("/test.db3", "/testResult_7.db3", PolicyType.LIFETIME_CACHE_POLICY, 20, SE_SIZE);

        //QUOTA POLICIES
        grid.process("/t.db3", "/tr1.db3", PolicyType.STATIC_QUOTA_POLICY, 20, 25600, 3);
        grid.process("/t.db3", "/tr2.db3", PolicyType.STATIC_QUOTA_POLICY, 20, 0, 1);
        grid.process("/t.db3", "/tr3.db3", PolicyType.ELASTIC_QUOTA_POLICY, 20, 25600, 3);
    }
}
