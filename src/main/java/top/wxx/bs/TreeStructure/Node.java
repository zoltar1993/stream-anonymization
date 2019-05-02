package top.wxx.bs.TreeStructure;

import java.util.ArrayList;

public class Node{
        
        private String data;
        private ArrayList<String> children;
        
        public Node(String data){
            this.data=data;
            this.children = new ArrayList<String>();
        }
        
        public String getData(){
            return this.data;
        }
        
        public ArrayList<String> getChildren(){
            return this.children;
        }
        
        public void addChild(String data){
            children.add(data);
        }
        
        public boolean isLeaf(){
            return this.children.size()==0;
        }
       
}