package gridsim.dms.policy;

/**
 *
 * @author Rafael Silva
 */
public class PolicyType {

    public static final int NO_POLICY = 0;
    
    // CACHE
    public static final int OLDEST_CACHE_POLICY = 1;
    public static final int LRU_CACHE_POLICY = 2;
    public static final int MOU_CACHE_POLICY = 3;
    public static final int SIZE_CACHE_POLICY = 4;

    // DELETE
    public static final int LIFETIME_POLICY = 11;
    public static final int LIFETIME_INCREASE_POLICY = 12;
    public static final int LIFETIME_INCREASE_CACHE_POLICY = 13;
    public static final int LIFETIME_CACHE_POLICY = 14;
    public static final int LIFETIME_CACHE_COUNT_POLICY = 15;
}
