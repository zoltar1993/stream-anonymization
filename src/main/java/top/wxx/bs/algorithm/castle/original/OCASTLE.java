package top.wxx.bs.algorithm.castle.original;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author 77781225
 */
public class OCASTLE {

    //Parameters
    int NTuples;// Number of tuples
    int Kanon; // anonymity degree
    int RTuples;// Total red tuples

    //Time constraints
    int Delta;// allowed time for tuple
    // allowed time for tuple
    int Beta;// allowed time for k-anonymous cluster
    double Tau;


    // Threads for the stream
    Thread Read;
    Thread Anonymize;
    Thread CheckKC;

    // trees and ranges
    AdultRange Ranges;
    AdultTree Trees;

    //Buffer
    BlockingQueue<Tuple> buffer;
    BlockingQueue<Tuple> readedBuffer;

    //Non-K anon clusters
    Vector<Cluster> Clusters;
    // K-anonymous clusters
    Vector<Cluster> KClusters;
    //Output
    Vector<AnonymizationOutput> Output;

    long startTime;

    DataAccessor dataAccessor = null;


    /**
     * @param N number of tuple
     * @param K anonymity degree
     * @param D Delta-windows size
     * @param B Beta- KCluster number
     */
    public OCASTLE(int N, int K, int D, int B){


        //main parameters
        this.NTuples = N;
        this.Kanon = K;
        this.Delta = D;
        this.Beta = B;
        // TAU
        this.Tau = 0.0;


        dataAccessor = null;

        //Buffer
        buffer = new LinkedBlockingDeque<>();
        readedBuffer = new LinkedBlockingDeque<>();

        //Non-K anon clusters
        Clusters = new Vector<Cluster>();

        //K-anonymous clusters
        KClusters = new Vector<Cluster>();

        //Create Range
        Ranges = new AdultRange();

        //Output
        Output = new Vector<AnonymizationOutput>();
        // Reading phase
        Read = new Thread() {
            @Override
            public void run(){
                try{
                    RTuples = 0;
                    List<Tuple> tuples = dataAccessor.getAllTuple(NTuples);
                    for( Tuple tuple : tuples ){
                        // englarge Ranges;
                        Ranges.ageRange.enlargeRange(tuple.age);
                        Ranges.fhlweightRange.enlargeRange(tuple.fhlweight);
                        Ranges.edu_numRange.enlargeRange(tuple.education_num);
                        Ranges.hours_weekRange.enlargeRange(tuple.hour_per_week);

                        buffer.offer(tuple);
//                    sleep(5);
                        RTuples++;
                        System.out.println(tuple.toString());
                    }

                } catch( Exception ex ){
                    Logger.getLogger(OCASTLE.class.getName()).log(Level.SEVERE, null, ex);
                }
                System.out.println("reading fnished reading fnished reading fnished reading fnishedreading fnishedreading fnishedreading fnishedreading fnished");
            }
        };// read phase

        //Anonymizing phase
        Anonymize = new Thread() {

            @Override
            public void run(){

                try{
                    // sleep one second for wait
                    Thread.sleep(1000);
                } catch( InterruptedException e ){
                    e.printStackTrace();
                }

                while( true ){
                    Tuple t = new Tuple();
                    Cluster c;

                    t = take(buffer);

                    c = BestSelection(t);
                    if( c == null ){
                        Cluster cl = new Cluster(Ranges, t);
                        Clusters.add(cl);
                    } else {
                        c.addTuple(t);
                    }
                    readedBuffer.offer(t);


                    if( readedBuffer.size() > Delta ){
                        DelayConstraint( take(readedBuffer) );
                    }
                }
            }
        };
    }//Constructor ends here...

    private Tuple take(BlockingQueue<Tuple> buffer){
        Tuple t = null;

        try{
            t = buffer.take();
        } catch( InterruptedException e ){
            throw new RuntimeException("从buffer中读取tuple异常");
        }

        return t;
    }


    public void startAll(){
        Read.start();
        Anonymize.start();
        // CheckKC.start();
    }

    public void setDataAccessor(DataAccessor dataAccessor){
        this.dataAccessor = dataAccessor;
    }


    /**
     * Find best Cluster for tuple T which has less Information loss that Tau
     *
     * @param T Tuple
     * @return Best cluster for Tuple or NULL
     */
    Cluster BestSelection(Tuple T){
        int currentsize = Clusters.size();
        EnlargeCost[] E = new EnlargeCost[currentsize];

        double mine = Double.MAX_VALUE;
        double e;
        for( int i = 0 ; i < currentsize; i++ ){
            e = Clusters.get(i).Enlargement(T);
            E[i] = new EnlargeCost(i, e);
            if( e < mine ) mine = e;
        }

        ArrayList<Integer> SetCmin = new ArrayList<Integer>();
        for( int i = 0; i < currentsize; i++ ){
            if( E[i].cost == mine ) SetCmin.add(E[i].index);
        }

        ArrayList<Integer> SetOk = new ArrayList<Integer>();
        for( int i = 0; i < SetCmin.size(); i++ ){
            double IL_Cj = Clusters.get(SetCmin.get(i)).ILAfterAddTuple(T);
            if( IL_Cj <= Tau ){
                SetOk.add(SetCmin.get(i));
            }
        }


        Random rd = new Random();
        Cluster cl = null;
        if( SetOk.isEmpty() ){
            if( Clusters.size() >= Beta ){
                int index = rd.nextInt(SetCmin.size());
                cl = Clusters.get(SetCmin.get(index));
            }
        } else {
            int index = rd.nextInt(SetOk.size());
            cl = Clusters.get(index);
        }
        return cl;
    }//Best selection


    /**
     * Output expiring tuple
     *
     * @param T Tuple
     */
    public void DelayConstraint(Tuple T){
        Cluster C = FindClusterOfTuple(T);
        int size = C.getSize();

        if( size >= Kanon ){
            OutputCluster(C);
        } else {
            ArrayList<Cluster> KC_set = new ArrayList<Cluster>();
            for( Cluster Cl : KClusters ){
                if( Cl.isCovers(T) ) KC_set.add(Cl);
            }

            if( !KC_set.isEmpty() ){
                Random rd = new Random();
                int index = rd.nextInt(KC_set.size());
                Cluster Cl = KC_set.get(index);
                AnonymizationOutput Anony = new AnonymizationOutput(T, Cl);
                Output.add(Anony);
                C.tuples.remove(T);// After anonymizing should remove tuple from cluster
                return;
            }//Anonymize with existing Ks cluster
            else {
                int m = 0;
                int totalsize = 0;// size of the buffer (Summa(nonKS.size))
                for( Cluster Cl : Clusters ){
                    if( C.getSize() < Cl.getSize() ) m++;
                    size += Cl.getSize();
                }

                if( m > Clusters.size() / 2 || totalsize < Kanon ){
                    Cluster sup = getSuppressCluster();
                    AnonymizationOutput Anony = new AnonymizationOutput(T, sup);
                    C.tuples.remove(T);// After anonymizing should remove tuple from cluster
                }//supress and anonymize;
                else {
                    MergeClusters();
                    OutputCluster(Clusters.firstElement());
                }
            }

        }
    }

    /**
     * Find cluster of tuple T
     *
     * @param T Tuple
     * @return Cluster which contains T;
     */
    public Cluster FindClusterOfTuple(Tuple T){
        Cluster cl = null;
        int order = T.receivedOrder;
        for( Cluster Cluster : Clusters ){
            if( Cluster.isContains(order) ) return Cluster;
        }
        return cl;
    }

    /**
     * Output Cluster C
     *
     * @param C Cluster
     */
    public void OutputCluster(Cluster C){
        ArrayList<Cluster> SC = new ArrayList<Cluster>();
        if( C.getSize() >= 2 * Kanon ){
            SC = Split(C);
        } else SC.add(C);

        AnonymizationOutput anony = null;
        for( Cluster Cj : SC ){
            for( Tuple T : Cj.tuples ){
                anony = new AnonymizationOutput(T, Cj);
                Output.add(anony);
            }
            double IL = Cj.InfoLoss();
            if( IL > Tau ) Tau = IL;
            if( Tau > IL ){
                KClusters.add(Cj);
            }
        }
        Clusters.remove(C);//Remove anonymized cluster
    }

    /**
     * Split big clusters into several clusters
     *
     * @param C Cluster
     * @return cluster set
     */
    private ArrayList<Cluster> Split(Cluster C){
        ArrayList<Cluster> AC = new ArrayList<Cluster>();
        ArrayList<Bucket> BS = new ArrayList<Bucket>();
        ArrayList<Tuple> TS = C.tuples;//tuples of C

        Tuple t = TS.get(0);
        Bucket b = new Bucket(t);
        BS.add(b);

        // assign buckets 
        for( int i = 1; i < TS.size(); i++ ){
            int assigned = 0;
            Tuple td = TS.get(i);
            for( Bucket B : BS ){
                if( B.canInclude(td) ){
                    B.addTuple(td);
                    assigned = 1;
                }
            }
            if( assigned == 0 ){
                b = new Bucket(td);
                BS.add(b);
            }
        }// finished grouping tuples by pid into Buckets BS 
        Random rd = new Random();
        int index;
        while( BS.size() >= Kanon ){
            index = rd.nextInt(BS.size());
            Bucket B = BS.get(index);// select random bucket
            Tuple Tk;//Tk- find KNN of Tk
            if( B.getSize() == 1 ){
                Tk = B.tuples.get(0);
                BS.remove(B);
            } else {
                Tk = B.tuples.get(rd.nextInt(B.getSize()));
                B.removeTuple(Tk);
            }


        }


        return AC;
    }


    /**
     * @return return suppressed cluster
     */
    public Cluster getSuppressCluster(){
        Cluster SupCl = new Cluster(Ranges);
        SupCl.SuppressCluster();
        return SupCl;
    }

    /**
     * Merge remaining cluster into single cluster
     */
    private void MergeClusters(){
        Cluster MC = Clusters.get(0);
        Cluster TC = null;
        Clusters.remove(0);
        while( !Clusters.isEmpty() ){
            TC = Clusters.firstElement();
            for( Tuple t : TC.tuples ){
                MC.addTuple(t);
            }
            Clusters.remove(0);
        }
        Clusters.add(MC);
    }


}