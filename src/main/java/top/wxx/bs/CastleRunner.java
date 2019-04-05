package top.wxx.bs;

import top.wxx.bs.algorithm.castle.original.OCASTLE;

/**
 * Created by zoltar on 2019/4/6.
 */

public class CastleRunner {

    public static void main(String... args){
        int N = 20; // data/adultCASLE.csv 里有 16281 条数据，确定只用20条?
        int K = 4;
        int D = 10;
        int B = 6;

        OCASTLE ocastle = new OCASTLE(N, K, D, B);
        ocastle.startAll();
    }

}
