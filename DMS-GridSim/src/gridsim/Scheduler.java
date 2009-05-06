package gridsim;

import gridsim.dms.SE;
import gridsim.dms.policy.Policy;
import gridsim.dms.policy.delete.LRUPolicy;
import gridsim.dms.policy.delete.MOUPolicy;
import gridsim.dms.policy.delete.NoPolicy;
import gridsim.dms.policy.delete.OldestPolicy;
import java.util.List;
import java.util.Vector;

/**
 *
 * @author Rafael Silva
 */
public class Scheduler {

    private int time = 0;
    private int compareCode;
    private Workload wl;
    private Output out;
    private Cluster cluster;
    private SE se;

    public Scheduler(int compareCode, Workload wl, Output out, Cluster cluster, SE se) {
        this.compareCode = compareCode;
        this.wl = wl;
        this.out = out;
        this.cluster = cluster;
        this.se = se;
    }

    public void run() throws Exception {
        List<Node> freeNodes, busyNodes;

        Job job = wl.getJob();
        freeNodes = cluster.getNodes();
        busyNodes = new Vector<Node>(cluster.getSize());
        boolean scheduled = false;

        while (job != null) {

            if (job.getSubmitTime() > time) {
                time = job.getSubmitTime();
            }

            for (Node node : freeNodes) {
                if (node.getStatus().equals(Node.Status.available)) {
                    busyNodes.add(node);
                    System.out.println("SCHEDULED: " + job.getJobId());

                    Policy policy;

                    switch (compareCode) {
                        case 1:
                            policy = new OldestPolicy(se, job);
                            break;
                        case 2:
                            policy = new LRUPolicy(se, job);
                            break;
                        case 3:
                            policy = new MOUPolicy(se, job);
                            break;
                        default:
                            policy = new NoPolicy(se, job);
                    }

                    int totalRunTime = policy.getTotalRunTime(time);
                    job.setTotalRunTime(totalRunTime);
                    job.setWaitedTime(time - job.getSubmitTime());
                    job.setFinishTime(time + totalRunTime);
                    node.runJob(job);
                    freeNodes.remove(node);
                    scheduled = true;
                    break;
                }
            }

            if (!scheduled) {
                List<Node> cleanNodes = new Vector<Node>(busyNodes.size());
                while (true) {
                    for (Node node : busyNodes) {
                        if (node.getJob().getFinishTime() < time) {
                            Job finishedJob = node.finishJob();
                            //TODO: write Output
                            System.out.println("FINISHED JOB: " + finishedJob.getJobId());
                            out.write(finishedJob);
                            cleanNodes.add(node);
                        }
                    }
                    if (cleanNodes.size() > 0) {
                        for (Node node : cleanNodes) {
                            busyNodes.remove(node);
                            freeNodes.add(node);
                        }
                        break; // BREAK-WHILE
                    } else {
                        time++;
                    }
                }
            }

            if (scheduled) {
                job = wl.getJob();
                scheduled = false;
            }
        }

        if (busyNodes.size() > 0) {
            for (Node node : busyNodes) {
                Job finishedJob = node.finishJob();
                System.out.println("FINISHED JOB: " + finishedJob.getJobId());
                out.write(finishedJob);
            }
        }
    }
}
