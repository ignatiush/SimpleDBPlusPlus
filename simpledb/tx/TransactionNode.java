package simpledb.tx;

/**
 * Created by ignatiush on 10/29/16.
 */
public class TransactionNode {
    private Transaction txn;
    private TransactionNode nextNode;
    private TransactionNode prevNode;
    private int count = 0;

    public TransactionNode(Transaction newTxn, TransactionNode next, TransactionNode prev){
        this.txn = newTxn;
        this.nextNode = next;
        this.prevNode = prev;
    }

    public Transaction getTransaction(){
        return this.txn;
    }

    public void setNextNode(TransactionNode next){
        this.nextNode = next;
    }

    public TransactionNode getNextNode(){
        return this.nextNode;
    }

    public void setPrevNode(TransactionNode prev){
        this.prevNode = prev;
    }

    public TransactionNode getPrevNode(){
        return this.prevNode;
    }

    public int getCount() {
        return this.count;
    }

    public void setCount(int value){
        this.count = value;
    }
}
