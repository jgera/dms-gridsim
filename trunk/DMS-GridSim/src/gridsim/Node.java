package gridsim;

/**
 *
 * @author Rafael Silva
 */
public class Node {

    private int id;
    private Status status;
    private Job job;

    public static enum Status {

        running, available;
    }

    public Node(int id) {
        this.id = id;
        this.status = Status.available;
    }

    public Status getStatus() {
        return status;
    }

    public Job getJob() {
        return job;
    }

    public void runJob(Job job) {
        this.job = job;
        this.status = Status.running;
    }

    public Job finishJob() {
        this.status = Status.available;
        return this.job;
    }
}
