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
public class OutPut {

    private Connection conn;
    private PreparedStatement stat;

    public void create(String fileName) throws SQLException {
        File file = new File(fileName);

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
            stat.execute("CREATE TABLE Jobs (JobID INTEGER, SubmitTime INTEGER, WaitTime INTEGER, JobRunTime INTEGER, TotalRunTime INTEGER, DataID INTEGER, DataSize INTEGER);");
            conn.commit();
        } catch (Exception e) {
        }
    }

    public void close() throws SQLException {
        stat.executeBatch();
        conn.commit();
        conn.close();
    }
}
