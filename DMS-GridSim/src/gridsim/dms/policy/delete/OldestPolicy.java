package gridsim.dms.policy.delete;

import gridsim.Cluster;
import gridsim.Job;
import gridsim.Node;
import gridsim.Output;
import gridsim.Workload;
import gridsim.dms.SE;
import gridsim.dms.policy.Policy;
import gridsim.dms.util.DataTransfer;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

/**
 *
 * @author Rafael Silva
 */
public class OldestPolicy extends Policy {

    private int time = 0;
    private Workload wl;
    private Output out;
    private Cluster cluster;
    private SE se;

    public OldestPolicy(Workload wl, Output out, Cluster cluster, SE se) {
        this.wl = wl;
        this.out = out;
        this.cluster = cluster;
        this.se = se;
    }

    @Override
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

                    // Storage Element
                    int totalRunTime;
                    int dataSize = job.getData().getSize();

                    if (se.hasData(job.getData().getId())) {
                        System.out.println("---------------------- REUSE");
                        totalRunTime = job.getRunTime()
                                + DataTransfer.intranet(dataSize);
                    } else {
                        if (se.getAvailableSpace() < dataSize) {
                            System.out.println("-- SE FULL - DELETE - " + time);
                            Collections.sort(se.getDatas());
                            se.deleteData(dataSize);
                        }
                        job.getData().setCreationDate(time);
                        se.store(job.getData());
                        totalRunTime = job.getRunTime()
                                + DataTransfer.extranet(dataSize)
                                + DataTransfer.intranet(dataSize);
                    }

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
                //TODO: write Output
                System.out.println("FINISHED JOB: " + finishedJob.getJobId());
                out.write(finishedJob);
            }
        }
    }

}
