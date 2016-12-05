package simpledb.remote;

import simpledb.tx.Transaction;
import simpledb.query.Plan;
import simpledb.server.SimpleDB;
import simpledb.vc.versionControl;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.Object;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The RMI server-side implementation of RemoteStatement.
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
class RemoteStatementImpl extends UnicastRemoteObject implements RemoteStatement {
   private RemoteConnectionImpl rconn;
   
   public RemoteStatementImpl(RemoteConnectionImpl rconn) throws RemoteException {
      this.rconn = rconn;
   }

   public void executeVCcmd(String cmd){
      versionControl vcObj = new versionControl();
      File check = new File(SimpleDB.fileMgr().getDbDirectory().getAbsolutePath() + "/.vcObj/myState.ser");
      if (check.exists()) {
         try {
            FileInputStream inFile = new FileInputStream(SimpleDB.fileMgr().getDbDirectory().getAbsolutePath() + "/.vcObj/myState.ser");
            ObjectInputStream inObject = new ObjectInputStream(inFile);
            vcObj = (versionControl) inObject.readObject();
            inObject.close();
         } catch (FileNotFoundException e) {
            e.printStackTrace();
         } catch (IOException e) {
            e.printStackTrace();
         } catch (ClassNotFoundException e) {
            e.printStackTrace();
         }
      }
      vcObj.init(SimpleDB.fileMgr());
      String[] commands = cmd.split(" ", 2);
      switch (commands[0]){
         case "commit":
            vcObj.commit(commands[1]);
            break;
         case "checkout":
            vcObj.checkout(commands[1]);
            SimpleDB.planner().resetTransactions();
            break;
         default:
            break;
      }

      try {
         FileOutputStream outFile = new FileOutputStream(SimpleDB.fileMgr().getDbDirectory().getAbsolutePath() + "/.vcObj/myState.ser");
         ObjectOutputStream outObject = new ObjectOutputStream(outFile);
         outObject.writeObject(vcObj);
         outObject.close();
      } catch (FileNotFoundException e) {
         e.printStackTrace();
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   /**
    * Executes the specified SQL query string.
    * The method calls the query planner to create a plan
    * for the query. It then sends the plan to the
    * RemoteResultSetImpl constructor for processing.
    * @see simpledb.remote.RemoteStatement#executeQuery(java.lang.String)
    */
   public RemoteResultSet executeQuery(String qry) throws RemoteException {
      try {
         Transaction tx = rconn.getTransaction();
         Plan pln = SimpleDB.planner().createQueryPlan(qry, tx);
         return new  RemoteResultSetImpl(pln, rconn);
      }
      catch(RuntimeException e) {
         rconn.rollback();
         throw e;
      }
   }
   
   /**
    * Executes the specified SQL update command.
    * The method sends the command to the update planner,
    * which executes it.
    * @see simpledb.remote.RemoteStatement#executeUpdate(java.lang.String)
    */
   public int executeUpdate(String cmd) throws RemoteException {
      int result;
      Transaction tx = rconn.getTransaction();
      if ((cmd.startsWith("commit")) || (cmd.startsWith("checkout"))){
         executeVCcmd(cmd);
         rconn.commit();
         return 0;
      }else if (cmd.startsWith("undo")) {
         result = SimpleDB.planner().executeUndo(tx);
         tx.dontCommit();
         rconn.commit();
         return result;
      } else if (cmd.startsWith("redo")) {
         try{
            Transaction redoTx = SimpleDB.planner().executeRedo(tx);
            if (redoTx == null)
               return 0;
            result = SimpleDB.planner().executeUpdate(redoTx.getCmd(), redoTx);
            redoTx.commit();
            //rconn.commit();
            return result;
         } catch (RuntimeException e) {
            rconn.rollback();
            throw e;
         }
      } else {
         try {
            tx.setCmd(cmd);
            result = SimpleDB.planner().executeUpdate(cmd, tx);
            rconn.commit();
            return result;
         } catch (RuntimeException e) {
            rconn.rollback();
            throw e;
         }
      }
   }
}
