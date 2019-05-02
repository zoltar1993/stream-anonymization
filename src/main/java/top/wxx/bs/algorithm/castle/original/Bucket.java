package top.wxx.bs.algorithm.castle.original;

import java.util.ArrayList;

public class Bucket {
    int pid;
    ArrayList<Tuple> tuples;

    public Bucket(Tuple T) {
        this.tuples=new ArrayList<Tuple>();
        this.tuples.add(T);
        this.pid=T.getPid();
    }
    
   public int getSize(){
       return this.tuples.size();
   }
   
   public boolean canInclude(Tuple T){
        return T.pid==this.pid;
   }
   
   public void addTuple(Tuple T){
       this.tuples.add(T);
   }
   public void removeTuple(Tuple T){
       this.tuples.remove(T);
   }
}
