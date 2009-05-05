package gridsim;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 *
 * @author Rafael Silva
 */
public class Workload {

    private final int MIN_DATA_SIZE = 10; // in MB
    private final int MAX_DATA_SIZE = 1024; // in MB
    private final int MIN_RUNTIME = 300; // in seconds
    private final int MAX_RUNTIME = 1500; // in seconds
    private Connection conn;
    private PreparedStatement stat;
    private Random random;

    public Workload(String fileName, int numOfJobs, double reuse) throws Exception {

        this.createDatabase(fileName);

        int id = 1, count = 0;
        int submissionDelay = 30; // in seconds
        int submitTime = 0; // in seconds

        double threshold = numOfJobs * (reuse / 100);
        int[] datas = new int[(int) threshold];

        // Array of Data Size
        for (int i = 0; i < datas.length; i++) {
            random = new Random(System.currentTimeMillis());
            datas[i] = MIN_DATA_SIZE + random.nextInt(MAX_DATA_SIZE - MIN_DATA_SIZE);
        }

        int dataId = 1;
        int lastSubmitTime = 0;

        for (int i = 0; i < numOfJobs; i++) {

            try {
                random = new Random(System.currentTimeMillis());
                int runTime = MIN_RUNTIME + random.nextInt(MAX_RUNTIME - MIN_RUNTIME);

                if (id >= threshold) {
                    dataId = 1 + random.nextInt(datas.length - 1);
                    if (submitTime > lastSubmitTime) {
                        lastSubmitTime = submitTime;
                    }
                    submitTime = random.nextInt(lastSubmitTime);
                }

                stat.setInt(1, id++);
                stat.setInt(2, submitTime);
                stat.setInt(3, runTime);
                stat.setInt(4, dataId++);
                stat.setInt(5, datas[dataId - 1]);
                stat.addBatch();

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

    private void createDatabase(String fileName) throws SQLException {

        File file = new File(fileName);
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
