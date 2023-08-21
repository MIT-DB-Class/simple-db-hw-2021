package simpledb.optimizer;

import javafx.scene.SubScene;

public class BinaryIndexedTree {
    int[] tr;
    int cnt;
    int n;
    public BinaryIndexedTree(int n){
        tr = new int[n + 1];
        this.n = n;
        cnt = 0;
    }
    public void add(int ind, int val){

        while(ind <= n){
            tr[ind] += val;
            ind += (-ind&ind);
        }
        cnt ++ ;
    }
    public long query(int ind){
        long res = 0;
        if(ind > n) return cnt;
        while(ind > 0){
            res += (long) tr[ind];
            ind -= (-ind&ind);
        }
        return res;
    }
    public long range(int left, int right){
        return query(right) - query(left - 1);
    }

    public long index(int ind){
        if(ind == 0){
            return query(0);
        }else{
            return query(ind) - query(ind - 1);
        }
    }
}
