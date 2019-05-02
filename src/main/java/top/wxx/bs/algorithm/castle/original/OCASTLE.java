package top.wxx.bs.algorithm.castle.original;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class OCASTLE {
    final private static Logger logger = LoggerFactory.getLogger(OCASTLE.class);

    private Castle castle;

    int clusterId = 0;

    BlockingQueue<Tuple> buffer;
    BlockingQueue<AnonymizationOutput> outputBuffer;

    // Threads for the stream
    Thread ReadThread;
    Thread AnonymizeThread;
    Thread OutputThread;
    DataAccessor dataAccessor = null;

    /**
     * @param K anonymity degree
     * @param D Delta-windows size
     * @param B Beta- KCluster number
     */
    public OCASTLE(int K, int D, int B, double T){
        this.castle = new Castle(K,D,B,T);

        dataAccessor = null;

        //Buffer
        buffer = new LinkedBlockingDeque<>();
        //Output
        outputBuffer = new LinkedBlockingDeque<>();

        // Reading phase
        ReadThread = new Thread() {
            @Override
            public void run(){
                try{
                    List<Tuple> tuples = dataAccessor.getAllTuple();
                    for( Tuple tuple : tuples ){
                        tuple.receivedOrder = 0; //此处应有掌声！！！
                        buffer.offer(tuple);
                        logger.info(tuple.toString());
                    }

                } catch( Exception ex ){
                    logger.error("error when read tuple", ex);
                }
                logger.info("---------------   reading fnished   ---------------");
            }
        };

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

                    c = CastleFunc.selectBestCluster(t, castle);
                    if( c == null ){
                        Cluster cl = new Cluster(t);
                        cl.createdTime = clusterId++; //此处应有掌声！！！
                        castle.clusters.add(cl);
                    } else {
                        c.addTuple(t);
                    }
                    castle.readedBuffer.offer(t);

                    if( castle.readedBuffer.size() > castle.d ){
                        CastleFunc.delayConstraint(take(castle.readedBuffer), castle, outputBuffer);
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
    }

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

}