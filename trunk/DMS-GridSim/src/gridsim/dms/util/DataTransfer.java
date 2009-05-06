package gridsim.dms.util;

/**
 *
 * @author Rafael Silva
 */
public class DataTransfer {

    private static final float TRANSFER_INTRANET = 12.5f; // MBps
    private static final float TRANSFER_EXTRANET = 0.12f; // MBps

    public static int intranet(int dataSize) {
        return (int) (dataSize / TRANSFER_INTRANET);
    }

    public static int extranet(int dataSize) {
        return (int) (dataSize / TRANSFER_EXTRANET);
    }
}
