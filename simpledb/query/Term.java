package simpledb.query;

import simpledb.record.Schema;

/**
 * A term is a comparison between two expressions.
 * @author Edward Sciore
 *
 */
public class Term {
   /**
    * After a call to {@code Term()}, value is one of the following:
    * True means <=, >=, or =
    * False means nope
    * (think of inclusive like mathematical ranges (1,infinity) versus [1, infinity)
    */
   private boolean cmp_inclusive = false;

   /**
    * After a call to {@code Term()}, value is one of the following:
    * <ul>
    * <li>{@code CMP_NOTHING} indicates only "=" accepted
    * <li>{@code CMP_LESSTHAN} indicates a term lhs<rhs
    * <li>{@code CMP_GREATERTHAN} indicates a term lhs>rhs
    * </ul>
    * <p>
    * Should also check {@code cmp_inclusive} to see if it is inclusive
    */
   private int cmp_type = CMP_NOTHING;

   /**
    * Constant values
    */
   private static final int CMP_NOTHING = 0;
   private static final int CMP_LESSTHAN = 1;
   private static final int CMP_GREATERTHAN = 2;



   private Expression lhs, rhs;

   
   /**
    * Creates a new term that compares two expressions
    * for equality.
    * @param lhs  the LHS expression
    * @param rhs  the RHS expression
    */
   public Term(Expression lhs, Expression rhs, String predicateType) {
      // lhs and rhs can be of subclass FieldName or Constant
      this.lhs = lhs;
      this.rhs = rhs;

      if (predicateType.contains("=")) {
         cmp_inclusive = true;
      }
      if (predicateType.contains("<")) {
         cmp_type = CMP_LESSTHAN;
      } else if (predicateType.contains(">")) {
         cmp_type = CMP_GREATERTHAN;
      }
   }
   
   /**
    * Calculates the extent to which selecting on the term reduces 
    * the number of records output by a query.
    * For example if the reduction factor is 2, then the
    * term cuts the size of the output in half.
    * @param p the query's plan
    * @return the integer reduction factor.
    */
   public int reductionFactor(Plan p) {
      String lhsName, rhsName;
      if (lhs.isFieldName() && rhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         rhsName = rhs.asFieldName();
         return Math.max(p.distinctValues(lhsName),
                         p.distinctValues(rhsName));
      }
      if (lhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         return p.distinctValues(lhsName);
      }
      if (rhs.isFieldName()) {
         rhsName = rhs.asFieldName();
         return p.distinctValues(rhsName);
      }
      // otherwise, the term equates constants
      if (lhs.asConstant().equals(rhs.asConstant()))
         return 1;
      else
         return Integer.MAX_VALUE;
   }
   
   /**
    * Determines if this term is of the form "F=c"
    * where F is the specified field and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the constant or null
    */
   public Constant equatesWithConstant(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          rhs.isConstant())
         return rhs.asConstant();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               lhs.isConstant())
         return lhs.asConstant();
      else
         return null;
   }
   
   /**
    * Determines if this term is of the form "F1=F2"
    * where F1 is the specified field and F2 is another field.
    * If so, the method returns the name of that field.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the name of the other field, or null
    */
   public String equatesWithField(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          rhs.isFieldName())
         return rhs.asFieldName();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               lhs.isFieldName())
         return lhs.asFieldName();
      else
         return null;
   }
   
   /**
    * Returns true if both of the term's expressions
    * apply to the specified schema.
    * @param sch the schema
    * @return true if both expressions apply to the schema
    */
   public boolean appliesTo(Schema sch) {
      return lhs.appliesTo(sch) && rhs.appliesTo(sch);
   }
   
   /**
    * Returns true if both of the term's expressions
    * evaluate to the same constant,
    * with respect to the specified scan.
    * @param s the scan
    * @return true if both expressions have the same value in the scan
    */
   public boolean isSatisfied(Scan s) {
      // evaluate function handles field/constant
      Constant lhsval = lhs.evaluate(s);
      Constant rhsval = rhs.evaluate(s);

      int ret = rhsval.compareTo(lhsval);
      if (cmp_inclusive && ret==0) {
         return true;
      }
      if (cmp_type==CMP_GREATERTHAN && ret<0) {
         return true;
      } else if (cmp_type==CMP_LESSTHAN && ret>0) {
         return true;
      }

      return false;
   }
   
   public String toString() {
      if (cmp_inclusive) {
         if (cmp_type==CMP_GREATERTHAN) {
            return lhs.toString() + ">=" + rhs.toString();
         } else if (cmp_type==CMP_LESSTHAN) {
            return lhs.toString() + "<=" + rhs.toString();
         } else {
            return lhs.toString() + "=" + rhs.toString();
         }
      }
      if (cmp_type==CMP_GREATERTHAN) {
         return lhs.toString() + ">" + rhs.toString();
      } else if (cmp_type==CMP_LESSTHAN) {
         return lhs.toString() + "<" + rhs.toString();
      }
      return lhs.toString() + ":" + rhs.toString();
   }
}
