package simpledb.planner;

import javafx.scene.control.SelectionMode;
import javafx.scene.control.Tab;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.parse.*;
import simpledb.server.SimpleDB;
import java.util.*;

/**
 * The simplest, most naive query planner possible.
 * @author Edward Sciore
 */
public class BasicQueryPlanner implements QueryPlanner {

   /**
    * Creates a query plan as follows.  It first takes
    * the product of all tables and views; it then selects on the predicate;
    * and finally it projects on the field list.
    */
   public Plan createPlan(QueryData data, Transaction tx) {

      Plans pl = new Plans();

      //Step 1: Create a plan for each mentioned table or view
      List<Plan> plans = new ArrayList<Plan>();
      for (String tblname : data.tables()) {
         String viewdef = SimpleDB.mdMgr().getViewDef(tblname, tx);
         if (viewdef != null)
            plans.add(SimpleDB.planner().createQueryPlan(viewdef, tx));
         else {
            TablePlan plan = new TablePlan(tblname, tx);
            plans.add(plan);
            pl.addTablePlan(plan, tblname);
         }
      }

      //Step 2: Create the product of all table plans
      Plan p = plans.remove(0);
      int productsubNodeNumber = 2;
      boolean initial = true;
      for (Plan nextplan : plans) {
         p = new ProductPlan(p, nextplan);
         if (initial) {
            pl.addProductPlan((ProductPlan) p, 1, 2);
            initial = false;
         }
         else
            pl.addProductPlan((ProductPlan) p, productsubNodeNumber, pl.curr-1 );
         productsubNodeNumber++;
      }

      //Step 3: Add a selection plan for the predicate
      p = new SelectPlan(p, data.pred());
      pl.addSelectPlan((SelectPlan)p);

      //Step 4: Project on the field names
      p = new ProjectPlan(p, data.fields());
      pl.addProjectPlan((ProjectPlan) p);

      //Step 5: Rename fields
      p = new RenamePlan(p, data.as());
      pl.addRenamePlan((RenamePlan) p);

      boolean print = true;
      if (print)
         pl.print();

      return p;
   }

   private abstract class PlanListing {
      public Plan plan;
      public int number;
      public PlanListing(Plan plan, int number) {
         this.plan = plan;
         this.number = number;
      }
      public abstract void print_initial();
      public void print_tuples() {
         System.out.printf("Node %d Outputs:\n", number);
         System.out.println(printPlan(plan));
      }
      private String printPlan(Plan plan){
         String result = "";
         Schema schema = plan.schema();
         Scan scan = plan.open();
         Collection<String> fldnames = schema.fields();

         for (String fldname : fldnames) {
            result += fldname;
            result += ", ";
         }

         result = result.substring(0, result.length()-2); // rmv ,\s
         result += "\n";

         while (scan.next()) {
            result += "(";
            for (String fldname: fldnames) {
               Object value = scan.getVal(fldname).asJavaVal();
               result += value.toString();
               result += ", ";
            }
            result = result.substring(0, result.length()-2); // rmv ,\s
            result += ")\n";
         }

         scan.beforeFirst();

         return result;
      }
   }

   private class TablePlanListing extends PlanListing {
      public Plan plan;
      public int number;
      public String tblname;
      public TablePlanListing(Plan plan, int number, String tblname) {
         super(plan, number);
         this.plan = plan;
         this.number = number;
         this.tblname = tblname;
      }
      public void print_initial() {
         System.out.printf("Node %d: TablePlan on table %s\n", number, tblname);
      }

   }

   private class ProductPlanListing extends PlanListing {
      public Plan plan;
      public int number;
      public int part1;
      public int part2;
      public ProductPlanListing(Plan plan, int number, int part1, int part2) {
         super(plan, number);
         this.plan = plan;
         this.number = number;
         this.part1 = part1;
         this.part2 = part2;
      }
      public void print_initial() {
         System.out.printf("Node %d: ProductPlan from node %d, %d\n", number, part1, part2);
      }
   }

   private class SelectPlanListing extends PlanListing {
      public Plan plan;
      public int number;
      public int n2;
      public SelectPlanListing(Plan plan, int number, int n2) {
         super(plan, number);
         this.plan = plan;
         this.number = number;
         this.n2 = n2;
      }
      public void print_initial() {
         System.out.printf("Node %d: SelectPlan from node %d\n", number, n2);
      }
   }

   private class ProjectPlanListing extends PlanListing {
      public Plan plan;
      public int number;
      public int n2;
      public ProjectPlanListing(Plan plan, int number, int n2) {
         super(plan, number);
         this.plan = plan;
         this.number = number;
         this.n2 = n2;
      }
      public void print_initial() {
         System.out.printf("Node %d: ProjectPlan from node %d\n", number, n2);
      }
   }

   private class RenamePlanListing extends PlanListing {
      public Plan plan;
      public int number;
      public int n2;
      public RenamePlanListing(Plan plan, int number, int n2) {
         super(plan, number);
         this.plan = plan;
         this.number = number;
         this.n2 = n2;
      }
      public void print_initial() {
         System.out.printf("Node %d: RenamePlan from node %d\n", number, n2);
      }
   }

   private class Plans {
      private ArrayList<PlanListing> planlistings;
      private int curr;
      public Plans () {
         planlistings = new ArrayList<>();
         curr = 1;
      }
      public void addTablePlan (TablePlan plan, String tblname) {
         PlanListing pl = new TablePlanListing(plan, this.curr, tblname);
         planlistings.add(pl);
         this.curr++;
      }
      public void addProductPlan (ProductPlan plan, int part1, int part2){
         PlanListing pl = new ProductPlanListing(plan, this.curr, part1, part2);
         planlistings.add(pl);
         this.curr++;
      }
      public void addSelectPlan (SelectPlan plan) {
         PlanListing pl = new SelectPlanListing(plan, this.curr, this.curr -1);
         planlistings.add(pl);
         this.curr++;
      }
      public void addProjectPlan (ProjectPlan plan) {
         PlanListing pl = new ProjectPlanListing(plan, this.curr, this.curr -1);
         planlistings.add(pl);
         this.curr++;
      }
      public void addRenamePlan (RenamePlan plan) {
         PlanListing pl = new RenamePlanListing(plan, this.curr, this.curr -1);
         planlistings.add(pl);
         this.curr++;
      }
      void print () {
         for (PlanListing pl : planlistings) {
            pl.print_initial();
         }
         for (PlanListing pl : planlistings) {
            pl.print_tuples();
         }
      }
   }
}
