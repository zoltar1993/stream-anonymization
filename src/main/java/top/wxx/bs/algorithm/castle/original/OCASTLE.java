package top.wxx.bs.algorithm.castle.original;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;

public class OCASTLE {
    final private static Logger logger = LoggerFactory.getLogger(OCASTLE.class);

    private Castle castle;
    int clusterId = 0;

    DataAccessor dataAccessor = null;

    /**
     * @param K anonymity degree
     * @param D Delta-windows size
     * @param B Beta- KCluster number
     */
    public OCASTLE(int K, int D, int B, double T){
        this.castle = new Castle(K,D,B,T);
        this.dataAccessor = null;
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

    public void setDataAccessor(DataAccessor dataAccessor){
        this.dataAccessor = dataAccessor;
    }

    public void startAll(){
        List<Tuple> tuples = dataAccessor.getAllTuple();
        for(Tuple t : tuples){
            t.receivedOrder = 0; //此处应有掌声！！！
            logger.info(t.toString());

            Cluster c = CastleFunc.selectBestCluster(t, castle);
            if( c == null ){
                Cluster cl = new Cluster(t);
                cl.createdTime = clusterId++; //此处应有掌声！！！
                castle.clusters.add(cl);
            } else {
                c.addTuple(t);
            }
            castle.readedBuffer.offer(t);

            if( castle.readedBuffer.size() > castle.d ){
                List<AnonymizationOutput> output = CastleFunc.delayConstraint(take(castle.readedBuffer), castle);
                for(AnonymizationOutput ano : output){
                    logger.info("output : {}", ano);
                }
            }
        }
    }

}