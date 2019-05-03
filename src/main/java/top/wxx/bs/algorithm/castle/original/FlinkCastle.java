package top.wxx.bs.algorithm.castle.original;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

public class FlinkCastle {

    public static void main(String... args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);


        // ::::::::::   执行参数   ::::::::::

        final String filepath = params.get("filepath");                 // data file path
        final int parallelism = params.getInt("parallelism");      // 并行度

        final int k = params.getInt("k");                          // anonymity degree
        final int d = params.getInt("d");                          // allowed time for tuple
        final int b = params.getInt("b");                          // limit of Cluster.size()
        final double tau = params.has("tau") ? params.getDouble("tau") : 0.6;


        // ::::::::::   构造Castle实例   :::::::::
        final Castle castle = new Castle(k,d,b,tau);


        // 初始化执行执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);


        // ::::::::::::::::   执行CASTLE算法   :::::::::::::::
        DataStream<String> text = env.readTextFile(filepath);

        DataStream<Tuple> tuples = text.map(line -> DataConvertor.lineToTuple(line));

        final Integer clusterId = 0;
        DataStream<AnonymizationOutput> output = tuples
                .filter(t -> {
                    Cluster c = CastleFunc.selectBestCluster(t, castle);
                    if( c == null ){
                        c = new Cluster(t);
                        c.createdTime = clusterId + 1;
                        castle.clusters.add(c);
                    } else {
                        c.addTuple(t);
                    }

                    castle.readedBuffer.offer(t);
                    return castle.readedBuffer.size() > d;
                })
                .flatMap(new DataPublisher(castle));
//                .map(t -> new AnonymizationOutput(t, new Cluster(t)) );

        output.print().setParallelism(1);

        env.execute("Flink Castle Anonymization");
    }


    public static class DataPublisher implements FlatMapFunction<Tuple, AnonymizationOutput> {
        private Castle castle;

        public DataPublisher(Castle castle){ this.castle = castle; }

        @Override
        public void flatMap(Tuple t, Collector<AnonymizationOutput> out) throws Exception {
            System.out.println("flat Map , now tuple : " + t);
            System.out.println("readedBuffer site : " + castle.readedBuffer.size());
            Tuple preT = castle.readedBuffer.take();
            System.out.println("pass get preT ");
            List<AnonymizationOutput> oList = CastleFunc.delayConstraint(preT, castle);
            for(AnonymizationOutput o : oList) out.collect(o);
        }
    }
}