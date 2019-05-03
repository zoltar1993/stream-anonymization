package top.wxx.bs.algorithm.castle.original;

import java.io.Serializable;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by xiangxin.wang on 2019/5/2.
 */

public class Castle implements Serializable {
    public int k;                          // anonymity degree
    public int d;                          // allowed time for tuple
    public int b;                          // limit of Cluster.size()
    public double tau;

    public BlockingQueue<Tuple> readedBuffer = new LinkedBlockingDeque<>();
    public Vector<Cluster> clusters = new Vector<>();                // Non-K anon clusters
    public Vector<Cluster> kclusters = new Vector<>();               // K-anonymous clusters

    public Castle(int k, int d, int b, double tau){
        this.k = k;
        this.d = d;
        this.b = b;
        this.tau = tau;
    }

}
