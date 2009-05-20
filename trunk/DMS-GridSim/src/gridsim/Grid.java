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
            if (args.length == 0) {
//                grid.generateWorkload("workload/input/2input-100000-10.db3", 100000, 10);
                grid.generateWorkload("workload/input/2input-100000-25.db3", 100000, 25);
                grid.generateWorkload("workload/input/2input-100000-50.db3", 100000, 50);
                grid.generateWorkload("workload/input/2input-100000-75.db3", 100000, 75);
                grid.generateWorkload("workload/input/2input-100000-90.db3", 100000, 90);
            } else {
                String input = args[0];
                String output = args[1];
                int policy = new Integer(args[2]);
                int nodes = new Integer(args[3]);
                int seSize = new Integer(args[4]);

                grid.process(input, output, policy, nodes, seSize);
            }

//                int SE_SIZE = 2000; // 30%
//
//                grid.generateWorkload("/t.db3", 100, 50);
//                grid.process("/t.db3", "/tR1.db3", PolicyType.LIFETIME_CACHE_POLICY, 20, SE_SIZE);
//                grid.process("/t.db3", "/tR2.db3", PolicyType.LIFETIME_CACHE_COUNT_POLICY, 20, SE_SIZE);
//                grid.process("/t.db3", "/tR3.db3", PolicyType.LIFETIME_INCREASE_CACHE_POLICY, 20, SE_SIZE);

            // CACHE POLICIES
//                grid.process("/test.db3", "/testResult_1.db3", PolicyType.OLDEST_CACHE_POLICY, 20, SE_SIZE);
//                grid.process("/test.db3", "/testResult_2.db3", PolicyType.LRU_CACHE_POLICY, 20, SE_SIZE);
//                grid.process("/test.db3", "/testResult_3.db3", PolicyType.MOU_CACHE_POLICY, 20, SE_SIZE);
//                grid.process("/test.db3", "/testResult_4.db3", PolicyType.SIZE_CACHE_POLICY, 20, SE_SIZE);

            // DELETE POLICIES
//                grid.process("/test.db3", "/testResult_5.db3", PolicyType.LIFETIME_POLICY, 20, SE_SIZE);
//                grid.process("/test.db3", "/testResult_6.db3", PolicyType.LIFETIME_INCREASE_POLICY, 20, SE_SIZE);
//                grid.process("/test.db3", "/testResult_7.db3", PolicyType.LIFETIME_CACHE_POLICY, 20, SE_SIZE);

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
