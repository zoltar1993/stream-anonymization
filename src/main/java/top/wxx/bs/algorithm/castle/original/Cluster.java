/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package top.wxx.bs.algorithm.castle.original;

import java.util.ArrayList;

/**
 *
 * @author 77781225
 */
public class Cluster {
    
    //Descriptor
    long createdTime;
    Range ageRange;
    Range fhlweightRange;
    Range edu_numRange;
    Range hours_weekRange;
    
    String work_class;
    String education;
    String marital_status;
    String race;
    String gender;
    //Tuples in cluster
    ArrayList<Tuple> tuples;
    //Adult tree
    AdultRange Ranges;

    Cluster(){}

    Cluster(AdultRange rn) {
        this.Ranges=rn;
    }    
    
    Cluster(AdultRange rn,Tuple T) {
        this.Ranges=rn;
        
        this.createdTime = System.currentTimeMillis();
        this.ageRange =new Range(T.age,T.age);
        this.fhlweightRange = new Range(T.fhlweight, T.fhlweight);
        this.edu_numRange = new Range(T.education_num,T.education_num);
        this.hours_weekRange = new Range(T.hour_per_week,T.hour_per_week);
        this.work_class = T.work_class;
        this.education = T.education;
        this.marital_status = T.marital_status;
        this.race = T.race;
        this.gender = T.gender;
        tuples= new ArrayList<Tuple>();
        tuples.add(T);
    }    


    public void addTuple(Tuple T){
        if(isCovers(T)==false){
        this.ageRange.enlargeRange(T.age);
        this.fhlweightRange.enlargeRange(T.fhlweight);
        this.edu_numRange.enlargeRange(T.education_num);
        this.hours_weekRange.enlargeRange(T.hour_per_week);
        this.work_class=AdultTree.WorkClass.getLCA(this.work_class,T.work_class);
        this.education=AdultTree.Education.getLCA(this.education,T.education);
        this.marital_status=AdultTree.MaritalStatus.getLCA(this.marital_status,T.marital_status);
        this.race=AdultTree.Race.getLCA(this.race,T.race);
        this.gender=AdultTree.Gender.getLCA(this.gender,T.gender);
        }
        tuples.add(T);
    }
    
    //Enlargement
    /**
     * The enlargement function defined here.
     * @param C -Cluster
     * @param T -Tuple 
     * @return 
     */
    double Enlargement(Tuple T){
        double en=0.0;
        double o,g;
        Cluster C=this;
        C.addTuple(T);
        //age 
        o=this.ageRange.getRangeSize()/Ranges.ageRange.getRangeSize();
        g=C.ageRange.getRangeSize()/Ranges.ageRange.getRangeSize();
        en+=g-o;
        
        //fhlweight
        o=this.fhlweightRange.getRangeSize()/Ranges.fhlweightRange.getRangeSize();
        g=C.fhlweightRange.getRangeSize()/Ranges.fhlweightRange.getRangeSize();
        en+=g-o;
        
        //education num
        o=this.edu_numRange.getRangeSize()/Ranges.edu_numRange.getRangeSize();
        g=C.edu_numRange.getRangeSize()/Ranges.edu_numRange.getRangeSize();
        en+=g-o;
        
        //hours per week
        o=this.hours_weekRange.getRangeSize()/Ranges.hours_weekRange.getRangeSize();
        g=C.hours_weekRange.getRangeSize()/Ranges.hours_weekRange.getRangeSize();
        en+=g-o;
        
        //work class
        o=AdultTree.WorkClass.getLeafNumber(this.work_class)/AdultTree.WorkClass.getLeafNumber();
        g=AdultTree.WorkClass.getLeafNumber(C.work_class)/AdultTree.WorkClass.getLeafNumber();
        en+=g-o;
        
        //education
        o=AdultTree.Education.getLeafNumber(this.education)/AdultTree.Education.getLeafNumber();
        g=AdultTree.Education.getLeafNumber(C.education)/AdultTree.Education.getLeafNumber();
        en+=g-o;
        
        //marital status
        o=AdultTree.MaritalStatus.getLeafNumber(this.marital_status)/AdultTree.MaritalStatus.getLeafNumber();
        g=AdultTree.MaritalStatus.getLeafNumber(C.marital_status)/AdultTree.MaritalStatus.getLeafNumber();
        en+=g-o;
        
        //race
        o=AdultTree.Race.getLeafNumber(this.race)/AdultTree.Race.getLeafNumber();
        g=AdultTree.Race.getLeafNumber(C.race)/AdultTree.Race.getLeafNumber();
        en+=g-o;
        
        //gender
        o=AdultTree.Gender.getLeafNumber(this.gender)/AdultTree.Gender.getLeafNumber();
        g=AdultTree.Gender.getLeafNumber(C.gender)/AdultTree.Gender.getLeafNumber();
        en+=g-o;
        
        en=en/9.0;
        return en;
    }
    
    double ILAfterAddTuple(Tuple T){
       Cluster C=this;
       C.addTuple(T);
       return C.InfoLoss();
    }
    
    /**
     * Information loss of whole cluster ???
     * @return Information loss of whole cluster
     */
    double InfoLoss(){
        double IL=0.0;
        IL+=this.ageRange.getRangeSize()/Ranges.ageRange.getRangeSize();
        IL+=this.fhlweightRange.getRangeSize()/Ranges.fhlweightRange.getRangeSize();
        IL+=this.edu_numRange.getRangeSize()/Ranges.edu_numRange.getRangeSize();
        IL+=this.hours_weekRange.getRangeSize()/Ranges.hours_weekRange.getRangeSize();
        IL+=AdultTree.WorkClass.getLeafNumber(this.work_class)/AdultTree.WorkClass.getLeafNumber();
        IL+=AdultTree.Education.getLeafNumber(this.education)/AdultTree.Education.getLeafNumber();
        IL+=AdultTree.MaritalStatus.getLeafNumber(this.marital_status)/AdultTree.MaritalStatus.getLeafNumber();
        IL+=AdultTree.Race.getLeafNumber(this.race)/AdultTree.Race.getLeafNumber();
        IL+=AdultTree.Gender.getLeafNumber(this.gender)/AdultTree.Gender.getLeafNumber();
        
        //??
        IL=IL/9.0;
//        IL=IL*tuples.size();
        return IL;
    }
    
    
    /**
     * Confusing part 
     * @param T tuple in this cluster
     * @return InfoLoss of Tuple in Cluster
     */
    double InfoLoss(Tuple T){
        double IL=0.0;
        return IL;
    }
    
    /**
     * Check tuple assigned to this cluster
     * @param order received order of tuple
     * @return True if Cluster contains Tuple T
     */
    boolean isContains(int order){
        for(int i=0;i<tuples.size();i++){
            if(tuples.get(i).receivedOrder==order)return true;
        }
        return false;
    }
    
    boolean isCovers(Tuple T){
        
        // numeric           
        if(this.ageRange.isBelongs(T.age)==false)return false;
        if(this.fhlweightRange.isBelongs(T.fhlweight)==false)return false;
        if(this.edu_numRange.isBelongs(T.education_num)==false)return false;
        if(this.hours_weekRange.isBelongs(T.hour_per_week)==false)return false;

        //categorical
        if(AdultTree.Education.getLCA(this.education, T.education).equals(this.education)==false)return false;
        if(AdultTree.MaritalStatus.getLCA(this.marital_status, T.marital_status).equals(this.marital_status)==false)return false;
        if(AdultTree.WorkClass.getLCA(this.work_class, T.work_class).equals(this.work_class)==false)return false;
        if(AdultTree.Race.getLCA(this.race, T.race).equals(this.race)==false)return false;
        if(AdultTree.Gender.getLCA(this.gender, T.gender).equals(this.gender)==false)return false;

        return true;
    }
    
    
    int getSize(){
        return tuples.size();
    }
    
    
    @Override
    public String toString() {
         return "Tuple("+createdTime +")["+ageRange.toString()+"," +fhlweightRange.toString()+","+ edu_numRange.toString()+ ", "+
                                              hours_weekRange.toString()+", "+ work_class+", "+ education+", "+
                                              marital_status+", "+race+","+ gender+"]";
    } 

    public void SuppressCluster(){
        this.createdTime=-1;
        this.ageRange=Ranges.ageRange;
        this.fhlweightRange=Ranges.fhlweightRange;
        this.edu_numRange=Ranges.edu_numRange;
        this.hours_weekRange=Ranges.hours_weekRange;
        
        this.work_class = AdultTree.WorkClass.getRootName();
        this.education = AdultTree.Education.getRootName();
        this.marital_status = AdultTree.MaritalStatus.getRootName();
        this.race = AdultTree.Race.getRootName();
        this.gender = AdultTree.Gender.getRootName();
        tuples=null;
        Ranges=null;
    }
}
