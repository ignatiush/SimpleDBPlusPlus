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

   private TransactionNode head;
   private TransactionNode latest;
   private TransactionNode current;

   public int executeDelete(DeleteData data, Transaction tx) {
      TransactionNode txNode = newUpdate(tx);
      Plan p = new TablePlan(data.tableName(), tx);
      p = new SelectPlan(p, data.pred());
      UpdateScan us = (UpdateScan) p.open();
      int count = 0;
      while(us.next()) {
         us.delete();
         count++;
      }
      us.close();
      txNode.setCount(count);
      return count;
   }
   
   public int executeModify(ModifyData data, Transaction tx) {
      TransactionNode txNode = newUpdate(tx);
      Plan p = new TablePlan(data.tableName(), tx);
      p = new SelectPlan(p, data.pred());
      UpdateScan us = (UpdateScan) p.open();
      int count = 0;
      while(us.next()) {
         Constant val = data.newValue().evaluate(us);
         us.setVal(data.targetField(), val);
         count++;
      }
      us.close();
      txNode.setCount(count);
      return count;
   }
   
   public int executeInsert(InsertData data, Transaction tx) {
      TransactionNode txNode = newUpdate(tx);
      Plan p = new TablePlan(data.tableName(), tx);
      UpdateScan us = (UpdateScan) p.open();
      us.insert();
      Iterator<Constant> iter = data.vals().iterator();
      for (String fldname : data.fields()) {
         Constant val = iter.next();
         us.setVal(fldname, val);
      }
      us.close();
      txNode.setCount(1);
      return 1;
   }

   public int executeUndo(){
      if(current == head)
         return 0;
      current.getTransaction().rollback();
      int count = current.getCount();
      current = current.getPrevNode();

      return count;
   }

   public int executeRedo(Transaction tx){
      if (current == latest)
         return 0;
      current = current.getNextNode();
      current.getTransaction().commit();
      tx.dontCommit();
      return current.getCount();
   }

   private TransactionNode newUpdate(Transaction tx){
      TransactionNode newLatest = new TransactionNode(tx, null, latest);
      latest.setNextNode(newLatest);
      latest = newLatest;
      current = newLatest;
      return newLatest;
   }

   public int executeCreateTable(CreateTableData data, Transaction tx) {
      head = new TransactionNode(tx, null, null);
      latest = head;
      current = head;
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
