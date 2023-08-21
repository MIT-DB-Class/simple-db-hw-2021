package simpledb.transaction;

public class Lock {
    private TransactionId tid;
    private int lockType;
    public Lock(TransactionId tid, int lockType){
        this.tid = tid;
        this.lockType = lockType;
    }

    public int getLockType() {
        return lockType;
    }

    public TransactionId getTid() {
        return tid;
    }

    public void setLockType(int lockType) {
        this.lockType = lockType;
    }
}
