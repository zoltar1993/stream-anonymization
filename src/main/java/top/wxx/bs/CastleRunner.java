package top.wxx.bs;

import top.wxx.bs.algorithm.castle.original.FileDataAccessor;
import top.wxx.bs.algorithm.castle.original.OCASTLE;

/**
 * Created by zoltar on 2019/4/6.
 */

public class CastleRunner {

    public static void main(String... args){
        int K = 4;
        int D = 10;
        int B = 6;

        OCASTLE ocastle = new OCASTLE(K, D, B);

        ocastle.setDataAccessor( new FileDataAccessor() );

        ocastle.startAll();
    }

}
