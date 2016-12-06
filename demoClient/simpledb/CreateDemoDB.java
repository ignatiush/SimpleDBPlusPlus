import java.sql.*;
import simpledb.remote.SimpleDriver;

public class CreateDemoDB {
    public static void main(String[] args) {
		Connection conn = null;
		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();

			System.out.println("Creating PEOPLE table");
			String s = "create table PEOPLE(name varchar(20), ssn int, city varchar(20))";
			stmt.executeUpdate(s);

			System.out.println("Inserting into PEOPLE table");
			s = "insert into PEOPLE(name, ssn, city) values ";
			String[] pplevals = {	"('Devon', 0, 'Portland')",
									"('Pattie', 1, 'Austin')",
									"('Sixta', 2, 'Chicago')",
									"('Elvis', 3, 'Champaign')",
									"('Sommer', 4, 'Seattle')",
									"('Siobhan', 5, 'San Francisco')",
									"('Bradly', 6, 'Page')",
									"('Suanne', 7, 'New York City')",
									"('Fannie', 8, 'Urbana')",
									"('Rossana', 9, 'Austin')",
									"('Starla', 10, 'Chicago')",
									"('Candis', 11, 'Urbana')",
									"('Marna', 12, 'Urbana')",
									"('Karon', 13, 'New York City')",
									"('Mertie', 14, 'Portland')",
									"('Cortney', 15, 'Champaign')",
									"('Agustin', 16, 'Portland')",
									"('Doreatha', 17, 'Austin')",
									"('Teri', 18, 'New York City')",
									"('Skye', 19, 'Portland')",
									"('Dominga', 20, 'Champaign')",
									"('Marcus', 21, 'Seattle')",
									"('Shanika', 22, 'San Francisco')",
									"('Lauran', 23, 'San Francisco')",
									"('Louella', 24, 'San Francisco')",
									"('Doyle', 25, 'Urbana')",
									"('Thad', 26, 'Champaign')",
									"('Gilbert', 27, 'New York City')",
									"('Chere', 28, 'Chicago')",
									"('Sammy', 29, 'Chicago')",
									"('Scarlet', 30, 'Austin')",
									"('Belkis', 31, 'Austin')",
									"('Maura', 32, 'San Francisco')",
									"('Charlsie', 33, 'Seattle')",
									"('Gricelda', 34, 'New York City')",
									"('Johnny', 35, 'Seattle')",
									"('Edelmira', 36, 'New York City')",
									"('Lonna', 37, 'Page')",
									"('Leland', 38, 'Portland')",
									"('Alix', 39, 'Page')",
									"('Shon', 40, 'Portland')",
									"('Ester', 41, 'New York City')",
									"('Bronwyn', 42, 'New York City')",
									"('Randy', 43, 'Portland')",
									"('Hee', 44, 'Seattle')",
									"('Tomas', 45, 'San Francisco')",
									"('Minh', 46, 'San Francisco')",
									"('Aaron', 47, 'San Francisco')",
									"('Dora', 48, 'Portland')",
									"('Felipa', 49, 'Portland')"
								};

			for (int i=0; i<pplevals.length; i++)
				stmt.executeUpdate(s + pplevals[i]);
			


			System.out.println("Creating CITIES table");
			s = "create table CITIES(cname varchar(20), sname varchar(20), population int)";
			stmt.executeUpdate(s);

			System.out.println("Inserting into CITIES table");		
			s = "insert into CITIES(cname,sname,population) values ";
			String[] cityvals = {	"('Portland', 'Oregon', 609456)",
									"('Austin', 'Texas', 885400)",
									"('Chicago', 'Illinois', 2719012)",
									"('Champaign', 'Illinois', 83424)",
									"('Seattle', 'Washington', 652405)",
									"('San Francisco', 'California', 837442)",
									"('Page', 'Arizona', 7326)",
									"('New York City', 'New York', 8406201)",
									"('Urbana', 'Illinois', 41572)"
								};
			for (int i=0; i<cityvals.length; i++)
				stmt.executeUpdate(s + cityvals[i]);

			System.out.println("Creating STATE table");
			s = "create table STATE(state varchar(20), friendliness int)";
			stmt.executeUpdate(s);

			System.out.println("Inserting into STATE table");
			s = "insert into STATE(state, friendliness) values ";
			String[] friendlinessvals = {	"('Oregon', 1)",
											"('Texas', 2)",
											"('Washington', 4)",
											"('California', 5)",
											"('Arizona', 3)",
											"('New York', 2)",
											"('Illinois', 4)"
										};

			for (int i=0; i<friendlinessvals.length; i++)
				stmt.executeUpdate(s + friendlinessvals[i]);

		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}

