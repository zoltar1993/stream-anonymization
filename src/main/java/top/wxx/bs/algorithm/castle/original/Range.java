package top.wxx.bs.algorithm.castle.original;

public class Range {
    int Max;
    int Min;
    
    Range() {
        this.Max=0;
        this.Min=0;
    }

    public Range(int Max, int Min) {
        this.Max = Max;
        this.Min = Min;
    }
  
    /**
     * @param number check number in [Min,Max]
     * @return boolean value
     */
  
    public boolean isBelongs(int number){
        return number<=Max && number>=Min;
    }
    
    public void UpdateMax(int nmax){
            Max=nmax;
    }
    
    public void UpdateMin(int nmin){
            Min=nmin;
    }
    
    public double getRangeSize(){
        return (Max-Min)*1.0;
    }
    
    /**
     * enlarge range by - @param number   
     */
    public void enlargeRange(int number){
     if(Min==0 && Max==0) {
         Min=number;
         Max=number;     
     }   
        
     if(number<Min)Min=number;

     if(number>Max)Max=number;
    }

    @Override
    public String toString() {
        return "[ "+Min+" , "+Max+" ]";
    }
    
    
}
