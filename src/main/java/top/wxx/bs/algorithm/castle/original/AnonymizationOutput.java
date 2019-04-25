
package top.wxx.bs.algorithm.castle.original;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author 77781225
 */
public class AnonymizationOutput {
    Tuple tuple;
    Cluster cluster;
    double infoLoss;

    public AnonymizationOutput(Tuple tuple, Cluster cluster) {
        this.tuple = tuple;
        this.cluster = cluster;
        cluster.tuples=null;
        infoLoss = cluster.infoLoss();
    }

    @Override
    public String toString(){
        final StringBuffer sb = new StringBuffer("AnonymizationOutput{");
        sb.append("tuple=").append(tuple);
        sb.append(", cluster=").append(cluster);
        sb.append(", infoLoss=").append(infoLoss);
        sb.append('}');
        return sb.toString();
    }
}
