package gridsim;

import gridsim.dms.SE;
import gridsim.dms.policy.Policy;
import gridsim.dms.policy.delete.OldestPolicy;

/**
 *
 * @author Rafael Silva
 */
public class Grid {

    private Workload wl;

    public static void main(String[] args) {
        try {
            Grid grid = new Grid();
//            grid.generateWorkload("/test.db3", 1000, 50);
            grid.process("/test.db3", "/testResult.db3", 1, 20, 51200);

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
