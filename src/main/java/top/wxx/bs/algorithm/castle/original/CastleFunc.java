package top.wxx.bs.algorithm.castle.original;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by xiangxin.wang on 2019/5/2.
 */

public class CastleFunc {

    /**
     * Find best Cluster for tuple T which has less Information loss that Tau
     *
     * @param T Tuple
     * @return Best cluster for Tuple or NULL
     */
    private Cluster selectBestCluster(Tuple T, Castle castle){
        int currentsize = castle.clusters.size();
        EnlargeCost[] E = new EnlargeCost[currentsize];

        double mine = Double.MAX_VALUE;
        double e;
        for( int i = 0 ; i < currentsize; i++ ){
            e = castle.clusters.get(i).Enlargement(T);
            E[i] = new EnlargeCost(i, e);
            if( e < mine ) mine = e;
        }

        ArrayList<Integer> SetCmin = new ArrayList<Integer>();
        for( int i = 0; i < currentsize; i++ ){
            if( E[i].cost == mine ) SetCmin.add(E[i].index);
        }

        ArrayList<Integer> SetOk = new ArrayList<Integer>();
        for( int i = 0; i < SetCmin.size(); i++ ){
            double IL_Cj = castle.clusters.get(SetCmin.get(i)).ILAfterAddTuple(T);
            if( IL_Cj <= castle.tau ){
                SetOk.add(SetCmin.get(i));
            }
        }


        Random rd = new Random();
        Cluster cl = null;
        if( SetOk.isEmpty() ){
            if( castle.clusters.size() >= castle.b ){
                int index = rd.nextInt(SetCmin.size());
                cl = castle.clusters.get(SetCmin.get(index));
            }
        } else {
            int index = rd.nextInt(SetOk.size());
            cl = castle.clusters.get(index);
        }
        return cl;
    }

}
