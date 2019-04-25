package top.wxx.bs.algorithm.castle.original;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author 77781225
 */
public class OCASTLE {
    final private static Logger logger = LoggerFactory.getLogger(OCASTLE.class);

    //Parameters
    int Kanon; // anonymity degree

    //Time constraints
    int Delta;// allowed time for tuple
    // allowed time for tuple
    int Beta;// allowed time for k-anonymous cluster
    double Tau;


    // Threads for the stream
    Thread ReadThread;
    Thread AnonymizeThread;
    Thread OutputThread;

    //Buffer
    BlockingQueue<Tuple> buffer;
    BlockingQueue<Tuple> readedBuffer;

    //Non-K anon clusters
    Vector<Cluster> Clusters;
    // K-anonymous clusters
    Vector<Cluster> KClusters;
    //Output
    BlockingQueue<AnonymizationOutput> outputBuffer;


    DataAccessor dataAccessor = null;


    /**
     * @param K anonymity degree
     * @param D Delta-windows size
     * @param B Beta- KCluster number
     */
    public OCASTLE(int K, int D, int B){


        //main parameters
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

        //Output
        outputBuffer = new LinkedBlockingDeque<>();

        // Reading phase
        ReadThread = new Thread() {
            @Override
            public void run(){
                try{
                    List<Tuple> tuples = dataAccessor.getAllTuple();
                    for( Tuple tuple : tuples ){
                        buffer.offer(tuple);
                        logger.info(tuple.toString());
                    }

                } catch( Exception ex ){
                    logger.error("error when read tuple", ex);
                }
                logger.info("---------------   reading fnished   ---------------");
            }
        };// read phase

        //Anonymizing phase
        AnonymizeThread = new Thread() {

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
                        Cluster cl = new Cluster(t);
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

        OutputThread = new Thread(){
            @Override
            public void run(){
                while( true ){
                    try{
                        AnonymizationOutput output = takeOutput(outputBuffer);
                        logger.info("output : {}", output);
                    } catch( Exception e ){
                        logger.error("error in outputThread");
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

    private AnonymizationOutput takeOutput(BlockingQueue<AnonymizationOutput> buffer){
        AnonymizationOutput output = null;

        try{
            output = buffer.take();
        } catch( Exception e ) {
            throw new RuntimeException("从buffer中读取tuple异常");
        }

        return output;
    }


    public void startAll(){
        ReadThread.start();
        AnonymizeThread.start();
        OutputThread.start();
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

        if(C==null) return;

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
                outputBuffer.offer(Anony);
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
                outputBuffer.offer(anony);
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
     * @param C Cluster
     * @return cluster set
     */
    private ArrayList<Cluster> Split(Cluster C) {
        ArrayList<Cluster> AC=new ArrayList<Cluster>();
        ArrayList<Bucket> BS=new ArrayList<Bucket>();
        ArrayList<Tuple> TS=C.tuples;//tuples of C
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

        while(BS.size()>=Kanon){
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
     * @return return suppressed cluster
     */
    public Cluster getSuppressCluster(){
        Cluster SupCl = new Cluster();
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