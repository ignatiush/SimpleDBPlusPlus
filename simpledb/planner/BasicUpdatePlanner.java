package simpledb.planner;

import java.util.Iterator;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.tx.TransactionNode;
import simpledb.parse.*;
import simpledb.query.*;

/**
 * The basic planner for SQL update statements.
 * @author sciore
 */
public class BasicUpdatePlanner implements UpdatePlanner {

   private TransactionNode head = new TransactionNode(null, null, null);
   private TransactionNode latest = head;
   private TransactionNode current;

   public int executeDelete(DeleteData data, Transaction tx) {
      TransactionNode txNode = null;
      if (tx.isFirstUpdate())
         txNode = newUpdate(tx);
      else
         tx.txRedone();
      try {
         Plan p = new TablePlan(data.tableName(), tx);
         p = new SelectPlan(p, data.pred());
         UpdateScan us = (UpdateScan) p.open();
         int count = 0;
         while (us.next()) {
            us.delete();
            count++;
         }
         us.close();
         if(txNode != null)
            txNode.setCount(count);
         return count;
      } catch (Exception e) {
         rollbackNode();
         return -1;
      }
   }
   
   public int executeModify(ModifyData data, Transaction tx) {
      TransactionNode txNode = null;
      if (tx.isFirstUpdate())
         txNode = newUpdate(tx);
      else
         tx.txRedone();
      try {
         Plan p = new TablePlan(data.tableName(), tx);
         p = new SelectPlan(p, data.pred());
         UpdateScan us = (UpdateScan) p.open();
         int count = 0;
         while (us.next()) {
            Constant val = data.newValue().evaluate(us);
            us.setVal(data.targetField(), val);
            count++;
         }
         us.close();
         if(txNode != null)
            txNode.setCount(count);
         return count;
      } catch (Exception e) {
         rollbackNode();
         return -1;
      }
   }
   
   public int executeInsert(InsertData data, Transaction tx) {
      TransactionNode txNode = null;
      if (tx.isFirstUpdate())
         txNode = newUpdate(tx);
      else
         tx.txRedone();
      try {
         Plan p = new TablePlan(data.tableName(), tx);
         UpdateScan us = (UpdateScan) p.open();
         us.insert();
         Iterator<Constant> iter = data.vals().iterator();
         for (String fldname : data.fields()) {
            Constant val = iter.next();
            us.setVal(fldname, val);
         }
         us.close();
         if(txNode != null)
            txNode.setCount(1);
         return 1;
      }  catch (Exception e) {
         rollbackNode();
         return -1;
      }
   }

   public int executeUndo(Transaction tx){
      if (current == null || current == head)
         return 0;
      current.getTransaction().rollback();
      int count = current.getCount();
      current = current.getPrevNode();
      tx.dontCommit();
      tx.txUndone();
      return count;
   }

   public Transaction executeRedo(Transaction tx){
      if ((current != latest) && ((current == null) || (current.getNextNode() == null)))
         return null;
      else if (current == latest){
         if (current.getTransaction().getIsUnDone() == false)
            return null;
         else
            return current.getTransaction();
      }
      current = current.getNextNode();
      return current.getTransaction();
   }

   private void rollbackNode(){
      latest = latest.getPrevNode();
      latest.getNextNode().setPrevNode(null);
      latest.setNextNode(null);
      current = latest;
   }

   private TransactionNode newUpdate(Transaction tx){
      TransactionNode newLatest = new TransactionNode(tx, null, latest);
      if(latest != null)
         latest.setNextNode(newLatest);
      else
         head.setNextNode(newLatest);
      latest = newLatest;
      current = newLatest;
      tx.firstUpdateDone();
      return newLatest;
   }

   public void resetTransactions() {
      head = new TransactionNode(null, null, null);
      latest = head;
      current = null;
   }

   public int executeCreateTable(CreateTableData data, Transaction tx) {
//      head = new TransactionNode(tx, null, null);
//      latest = head;
//      current = head;
      SimpleDB.mdMgr().createTable(data.tableName(), data.newSchema(), tx);
      return 0;
   }

   public int executeCreateView(CreateViewData data, Transaction tx) {
      SimpleDB.mdMgr().createView(data.viewName(), data.viewDef(), tx);
      return 0;
   }
   public int executeCreateIndex(CreateIndexData data, Transaction tx) {
      SimpleDB.mdMgr().createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
      return 0;  
   }
}
