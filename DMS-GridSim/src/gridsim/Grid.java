package gridsim;

import gridsim.dms.SE;

/**
 *
 * @author Rafael Silva
 */
public class Grid {

    private Workload wl;

    public static void main(String[] args) {
        try {
            Grid grid = new Grid();
            grid.generateWorkload("/test.db3", 1000, 50);
            grid.process("/test.db3", "/testResult_0.db3", 0, 20, 51200);
            grid.process("/test.db3", "/testResult_1.db3", 1, 20, 51200);
            grid.process("/test.db3", "/testResult_2.db3", 2, 20, 51200);
            grid.process("/test.db3", "/testResult_3.db3", 3, 20, 51200);

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
