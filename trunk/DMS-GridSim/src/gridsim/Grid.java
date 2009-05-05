package gridsim;

/**
 *
 * @author Rafael Silva
 */
public class Grid {

    public static void main(String[] args) {
//        if (args[0].equals("-c")) {
            try {
                Workload wl = new Workload("/test.db3", 1000, 50);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
//        }
    }
}
