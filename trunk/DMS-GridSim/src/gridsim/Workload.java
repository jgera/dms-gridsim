package gridsim;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 *
 * @author Rafael Silva
 */
public class Workload {

    private final int MIN_TASKS = 1;
    private final int MAX_TASKS = 10;
    private final int MIN_DATA_SIZE = 10; // in MB
    private final int MAX_DATA_SIZE = 1024; // in MB
    private final int MIN_RUNTIME = 300; // in seconds
    private final int MAX_RUNTIME = 3000; // in seconds
    private File file;
    private Connection conn;
    private PreparedStatement stat;
    private Random random;
    private ResultSet rs;
    private int compareCode;

    public Workload(String fileName) {
        this.file = new File(fileName);
    }

    public void prepareToRead(int compareCode) throws Exception {
        this.compareCode = compareCode;

        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Missing SQLite JDBC library.");
        }

        conn = DriverManager.getConnection("jdbc:sqlite:" + file.getPath());
        Statement stat = conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
        conn.setAutoCommit(true);

        rs = stat.executeQuery("SELECT JobID, SubmitTime, RunTime, DataId, DataSize FROM Jobs ORDER BY SubmitTime;");
    }

    public Job getJob() throws Exception {
        if (!rs.next()) {
            return null;
        }
        return new Job(rs.getInt("JobID"), rs.getInt("SubmitTime"),
                rs.getInt("RunTime"), rs.getInt("DataId"),
                rs.getInt("DataSize"), this.compareCode);
    }

    public void generate(int numOfJobs, double reuse) throws Exception {

        this.createDatabase();

        int id = 1, count = 0;
        int submissionDelay = 30; // in seconds
        int submitTime = 0; // in seconds

        int threshold = numOfJobs - (int) (numOfJobs * (reuse / 100));
        int[] datas = new int[threshold];
        System.out.println("-- LIMIAR: " + threshold);
        // Array of Data Size
        for (int i = 0; i < threshold; i++) {
            random = new Random(System.nanoTime());
            datas[i] = MIN_DATA_SIZE + random.nextInt(MAX_DATA_SIZE - MIN_DATA_SIZE);
        }

        int dataId = 1;
        int lastSubmitTime = 0;

        for (int i = 0; i < numOfJobs; ) {
            try {
                random = new Random(System.nanoTime());

                int tasks = MIN_TASKS + random.nextInt(MAX_TASKS - MIN_TASKS);

                if (id > threshold) {
                    dataId = 1 + random.nextInt(threshold);
                    if (submitTime > lastSubmitTime) {
                        lastSubmitTime = submitTime;
                    }
                    submitTime = random.nextInt(lastSubmitTime);
                }

                for (int j = 0; j < tasks; j++, i++) {
                    int runTime = MIN_RUNTIME + random.nextInt(MAX_RUNTIME - MIN_RUNTIME);

                    stat.setInt(1, id++);
                    stat.setInt(2, submitTime);
                    stat.setInt(3, runTime);
                    stat.setInt(4, dataId);
                    stat.setInt(5, datas[dataId - 1]);
                    stat.addBatch();
                }
                dataId++;
                submitTime += submissionDelay;

                if (count == 1024) {
                    stat.executeBatch();
                    conn.commit();
                    count = 0;
                }

            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        }
        this.close();
    }

    private void createDatabase() throws SQLException {

        if (file.exists()) {
            file.delete();
        }

        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Missing SQLite JDBC library.");
        }

        conn = DriverManager.getConnection("jdbc:sqlite:" + file.getPath());
        Statement stat = conn.createStatement();
        conn.setAutoCommit(false);
        try {
            stat.execute("DROP TABLE Jobs;");
            conn.commit();
        } catch (Exception e) {
        }
        try {
            stat.execute("CREATE TABLE Jobs (JobID INTEGER, SubmitTime INTEGER, RunTime INTEGER, DataID INTEGER, DataSize INTEGER);");
            conn.commit();
        } catch (Exception e) {
        }

        this.stat = conn.prepareStatement("INSERT INTO Jobs VALUES(?, ?, ?, ?, ?)");
    }

    private void close() throws SQLException {
        stat.executeBatch();
        conn.commit();
        conn.close();
    }
}
