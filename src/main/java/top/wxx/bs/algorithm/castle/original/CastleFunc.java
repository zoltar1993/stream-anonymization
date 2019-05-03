package top.wxx.bs.algorithm.castle.original;

import java.util.*;
import java.util.stream.Collectors;

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
    static public Cluster selectBestCluster(Tuple T, Castle castle){
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

    /**
     * Output expiring tuple
     *
     * @param t Tuple
     */
    static public List<AnonymizationOutput> delayConstraint(Tuple t, Castle castle){
        List<AnonymizationOutput> output = new LinkedList<>();

        Cluster C = findClusterOfTuple(t, castle.clusters);

        if(C==null) return output;

        int size = C.getSize();

        if( size >= castle.k ){
            outputCluster(C, castle, output);
        } else {
            ArrayList<Cluster> KC_set = new ArrayList<Cluster>();
            for( Cluster Cl : castle.kclusters ){
                if( Cl.isCovers(t) ) KC_set.add(Cl);
            }

            if( !KC_set.isEmpty() ){
                Random rd = new Random();
                int index = rd.nextInt(KC_set.size());
                Cluster Cl = KC_set.get(index);
                if(t.receivedOrder == 0) {
                    AnonymizationOutput Anony = new AnonymizationOutput(t, Cl);
                    output.add(Anony);
                    t.receivedOrder = 1; //输出过的元组标记为1
                }
                C.tuples.remove(t);// After anonymizing should remove tuple from cluster
                return output;
            }//Anonymize with existing Ks cluster
            else {
                int m = 0;
                int totalsize = 0;// size of the buffer (Summa(nonKS.size))
                for( Cluster Cl : castle.clusters ){
                    if( C.getSize() < Cl.getSize() ) m++;
                    size += Cl.getSize();
                }

                if( m > castle.clusters.size() / 2 || totalsize < castle.k ){
                    Cluster sup = Cluster.getSuppressCluster();
                    if(t.receivedOrder == 0) {
                        AnonymizationOutput Anony = new AnonymizationOutput(t, sup);
                        output.add(Anony);
                        t.receivedOrder = 1; //输出过的元组标记为1
                    }
                    C.tuples.remove(t);// After anonymizing should remove tuple from cluster
                }//supress and anonymize;
                else {
                    mergeClusters(castle.clusters);
                    outputCluster(castle.clusters.firstElement(), castle, output);
                }
            }

        }

        return output;
    }


    /**
     * Find cluster of tuple T
     *
     * @param t Tuple
     * @return Cluster which contains T;
     */
    static private Cluster findClusterOfTuple(Tuple t, Vector<Cluster> clusters){
        for(Cluster c : clusters){
            if( c.isContains(t.pid) ) return c;
        }
        return null;
    }

    /**
     * Output Cluster C
     *
     * @param C Cluster
     */
    static private void outputCluster(Cluster C, Castle castle, List<AnonymizationOutput> output){
        ArrayList<Cluster> SC = new ArrayList<Cluster>();
        if( C.getSize() >= 2 * castle.k ){
            SC = Split(C, castle.k);
        } else {
            SC.add(C);
        }


        for( Cluster Cj : SC ){
            for( Tuple T : Cj.tuples ){
                if(T.receivedOrder == 0) {
                    output.add( new AnonymizationOutput(T, Cj) );
                    T.receivedOrder = 1; //输出过的元组标记为1
                }
            }
            castle.kclusters.add(Cj);
            castle.clusters.remove(Cj);
        }
    }

    /**
     * Split big clusters into several clusters
     * @param C Cluster
     * @return cluster set
     */
    static private ArrayList<Cluster> Split(Cluster C, int k) {
        ArrayList<Cluster> AC = new ArrayList<>();
        ArrayList<Bucket> BS = new ArrayList<>();
        ArrayList<Tuple> TS = C.tuples;//tuples of C
        Tuple t=TS.get(0);
        Bucket b=new Bucket(t);
        BS.add(b);
        // assign buckets
        for(int i=1;i<TS.size();i++){
            int assigned=0;
            Tuple td=TS.get(i);
            for(Bucket B: BS){
                if(B.canInclude(td)){
                    B.addTuple(td);
                    assigned=1;
                }
            }
            if(assigned==0){
                b=new Bucket(td);
                BS.add(b);
            }
        }// finished grouping tuples by pid into Buckets BS


        Random rd=new Random();
        int index;

        while(BS.size() >= k){
            index=rd.nextInt(BS.size());
            Bucket B=BS.get(index);// select random bucket
            Tuple Tk;//Tk- find KNN of Tk
            if(B.getSize()==1){
                Tk=B.tuples.get(0);
                BS.remove(B);
            }
            else {
                Tk=B.tuples.get(rd.nextInt(B.getSize()));
                B.removeTuple(Tk);
            }

            Cluster subC = new Cluster(Tk);
            subC.createdTime = -1; //此处。。。！！！
            if(B == null)
                BS.remove(B);

            for(int o = 0;o<B.getSize();o++) {
                Tuple Tb = B.tuples.get(o);
                subC.addTuple(Tb);
                B.tuples.remove(o);

                if(B == null)
                    BS.remove(B);
            }

            AC.add(subC);
            BS.remove(B);
        }
        double mine=Double.MAX_VALUE;
        EnlargeCost[] E=new EnlargeCost[AC.size()];
        for(int d=0;d<BS.size();d++) {
            Bucket Bi = BS.get(d);
            Tuple Ti=Bi.tuples.get(rd.nextInt(Bi.getSize()));
            for(int i=0;i<AC.size();i++) {
                double e=AC.get(i).ILAfterAddTuple(Ti);
                E[i]=new EnlargeCost(i,e);
                if(e<mine)mine=e;
            }

            for(int i=0;i<AC.size();i++) {

                if(E[i].cost==mine) {
                    for(int j= 0 ;j<Bi.getSize();j++) {
                        AC.get(E[i].index).addTuple(Bi.tuples.get(j));
                    }
                }
            }

            BS.remove(Bi);
        }
        return AC;
    }

    /**
     * Merge remaining cluster into single cluster
     */
    static private void mergeClusters(Vector<Cluster> clusters){
        List<Tuple> tuples = clusters.stream()
                .flatMap(c -> c.tuples.stream())
                .collect(Collectors.toList());
        Cluster mc = new Cluster(tuples.get(0));

        for(int i=1 ; i<tuples.size() ; i++){
            mc.addTuple( tuples.get(i) );
        }

        clusters.removeAllElements();

        clusters.add(mc);
    }

}
