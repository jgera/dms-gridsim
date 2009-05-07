package gridsim;

import gridsim.dms.SE;
import gridsim.dms.policy.Policy;
import gridsim.dms.policy.PolicyType;
import gridsim.dms.policy.delete.LifeTimePolicy;
import gridsim.dms.policy.NoPolicy;
import gridsim.dms.policy.delete.LifeTimeIncreasePolicy;
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
                    System.out.println("SCHEDULED: " + job.getJobId() + " - TIME: " + time);

                    Policy policy;

                    switch (compareCode) {
                        case PolicyType.LIFETIME_DELETE_POLICY:
                            policy = new LifeTimePolicy(se, job);
                            break;
                        case PolicyType.LIFETIME_INCREASE_DELETE_POLICY:
                            policy = new LifeTimeIncreasePolicy(se, job);
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
