package gridsim;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author Rafael Silva
 */
public class Output {

    private Connection conn;
    private PreparedStatement stat;

    public Output(String fileName) throws Exception {
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
            stat.execute("CREATE TABLE Jobs (JobID INTEGER, SubmitTime INTEGER, WaitTime INTEGER, RunTime INTEGER, TotalRunTime INTEGER, DataID INTEGER, DataSize INTEGER, UserId INTEGER);");
            conn.commit();
        } catch (Exception e) {
        }

        this.stat = conn.prepareStatement("INSERT INTO Jobs VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
    }

    public void write(Job job) throws Exception {
        try {
            stat.setInt(1, job.getJobId());
            stat.setInt(2, job.getSubmitTime());
            stat.setInt(3, job.getWaitedTime());
            stat.setInt(4, job.getRunTime());
            stat.setInt(5, job.getTotalRunTime());
            stat.setInt(6, job.getData().getId());
            stat.setInt(7, job.getData().getSize());
            stat.setInt(8, job.getData().getUserId());
            stat.addBatch();

            stat.executeBatch();
            conn.commit();

        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    public void close() throws SQLException {
        stat.executeBatch();
        conn.commit();
        conn.close();
    }
}
