package gridsim;

/**
 *
 * @author Rafael Silva
 */
public class Job {

    private int jobId;
    private int submitTime;
    private int runTime;
    private int totalRunTime;
    private int finishTime;
    private int waitedTime;
    private Data data;

    public Job(int jobId, int submitTime, int runTime, int dataId, int dataSize, int compareCode, int ownerId) {
        this.jobId = jobId;
        this.submitTime = submitTime;
        this.runTime = runTime;
        this.data = new Data(dataId, dataSize, compareCode, ownerId);
    }

    public Data getData() {
        return data;
    }

    public int getJobId() {
        return jobId;
    }

    public int getRunTime() {
        return runTime;
    }

    public int getSubmitTime() {
        return submitTime;
    }

    public int getTotalRunTime() {
        return totalRunTime;
    }

    public void setTotalRunTime(int totalRunTime) {
        this.totalRunTime = totalRunTime;
    }

    public int getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(int finishTime) {
        this.finishTime = finishTime;
    }

    public int getWaitedTime() {
        return waitedTime;
    }

    public void setWaitedTime(int waitedTime) {
        this.waitedTime = waitedTime;
    }
}
