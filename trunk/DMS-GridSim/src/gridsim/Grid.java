package gridsim;

/**
 *
 * @author Rafael Silva
 */
public class Grid {

    public static void main(String[] args) {
//        if (args[0].equals("-c")) {
            try {
                Workload wl = new Workload("workload/input/test.db3", 100, 50);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
//        }
    }
}
