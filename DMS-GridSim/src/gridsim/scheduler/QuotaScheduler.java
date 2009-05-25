package gridsim.scheduler;

import gridsim.Cluster;
import gridsim.Job;
import gridsim.Node;
import gridsim.Output;
import gridsim.Workload;
import gridsim.dms.SE;
import gridsim.dms.policy.Policy;
import gridsim.dms.policy.PolicyType;
import gridsim.dms.policy.quota.ElasticQuotaPolicy;
import gridsim.dms.policy.quota.StaticQuotaPolicy;
import java.util.List;
import java.util.Vector;

/**
 *
 * @author Rafael Silva
 */
public class QuotaScheduler implements Scheduler {

    public static final int NUMBER_OF_USERS = 10;
    public static int QUOTA_PER_USER; // in MB
    private int policyCode;
    private Workload workload;
    private Output out;
    private Cluster cluster;
    private int seListSize;
    private int seCapacity;

    public QuotaScheduler(int policyCode, Workload workload, Output out,
            Cluster cluster, int seListSize, int seCapacity) {
        this.policyCode = policyCode;
        this.workload = workload;
        this.out = out;
        this.cluster = cluster;
        this.seListSize = seListSize;
        this.seCapacity = seCapacity;
        QUOTA_PER_USER = (seListSize * seCapacity) / NUMBER_OF_USERS;
    }

    @Override
    public void run() throws Exception {

        // Configuring the SEs
        List<SE> seList = new Vector<SE>(seListSize - 1);
        int quotaPerUser = QUOTA_PER_USER / seListSize;
        for (int i = 0; i < seListSize - 1; i++) {
            seList.add(new SE(seCapacity, quotaPerUser));
        }
        SE localSE = new SE(seCapacity, quotaPerUser);

        List<Node> freeNodes = cluster.getNodes();
        List<Node> busyNodes = new Vector<Node>(cluster.getSize());

        Job job = workload.getJob();
        int time = 0;

        while (job != null) {
            boolean scheduled = false;

            if (job.getSubmitTime() > time) {
                time = job.getSubmitTime();
            }
            for (Node node : freeNodes) {
                if (node.getStatus().equals(Node.Status.available)) {
                    busyNodes.add(node);
                    System.out.println("SCHEDULED: " + job.getJobId() + " - TIME: " + time);

                    Policy policy;
                    switch (policyCode) {
                        case PolicyType.ELASTIC_QUOTA_POLICY:
                            policy = new ElasticQuotaPolicy(localSE, seList, job);
                            break;
                        default: // PolicyType.STATIC_QUOTA_POLICY:
                            policy = new StaticQuotaPolicy(localSE, seList, job);
                    }
                    int totalRunTime = policy.getTotalRunTime(time);
                    job.setTotalRunTime(totalRunTime);
                    job.setWaitedTime(time - job.getSubmitTime());
                    job.setFinishTime(time + totalRunTime);
                    node.runJob(job);
                    freeNodes.remove(node);

                    scheduled = true;
                    job = workload.getJob();
                    break;
                }
            }

            if (!scheduled) {
                List<Node> cleanNodes = new Vector<Node>(busyNodes.size());
                while (true) {
                    int nextEndJobTime = Integer.MAX_VALUE;
                    for (Node node : busyNodes) {
                        int nodeFinishTime = node.getJob().getFinishTime();
                        if (nodeFinishTime < time) {
                            Job finishedJob = node.finishJob();
                            System.out.println("FINISHED JOB: " + finishedJob.getJobId());
                            out.write(finishedJob);
                            cleanNodes.add(node);
                        } else if (nodeFinishTime < nextEndJobTime) {
                            nextEndJobTime = nodeFinishTime;
                        }
                    }
                    if (cleanNodes.size() > 0) {
                        for (Node node : cleanNodes) {
                            busyNodes.remove(node);
                            freeNodes.add(node);
                        }
                        break; // BREAK-WHILE
                    } else {
                        time = nextEndJobTime + 1;
                    }
                }
            }
        }
        // No more jobs to process
        if (busyNodes.size() > 0) {
            for (Node node : busyNodes) {
                Job finishedJob = node.finishJob();
                System.out.println("FINISHED JOB: " + finishedJob.getJobId());
                out.write(finishedJob);
            }
        }
    }
}
