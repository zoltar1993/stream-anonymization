package top.wxx.bs;

import org.apache.flink.api.java.utils.ParameterTool;
import top.wxx.bs.algorithm.castle.original.FileDataAccessor;
import top.wxx.bs.algorithm.castle.original.OCASTLE;

/**
 * Created by xiangxin.wang on 2019/4/6.
 */

public class CastleRunner {

    public static void main(String... args){
        final ParameterTool params = ParameterTool.fromArgs(args);

        final int k = params.getInt("k");                          // anonymity degree
        final int d = params.getInt("d");                          // allowed time for tuple
        final int b = params.getInt("b");                          // limit of Cluster.size()
        final double tau = params.has("tau") ? params.getDouble("tau") : 0.6;
        final int n = params.has("n") ? params.getInt("n") : 15318;

        OCASTLE ocastle = new OCASTLE(k, d, b, tau);

        ocastle.setDataAccessor( new FileDataAccessor(n) );

        ocastle.startAll();
    }

}
