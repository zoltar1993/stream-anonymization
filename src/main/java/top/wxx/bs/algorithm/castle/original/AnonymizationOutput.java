package top.wxx.bs.algorithm.castle.original;

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
