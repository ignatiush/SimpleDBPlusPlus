# SimpleDBPlusPlus
CS411 Database Systems Project @ UIUC

Hi there, welcome to our CS 411, Database Systems project. To get started, clone all download this repository from the newmaster branch and follow the instructions below.

1) 	Run 'chmod +x [script_name]' for the provided shell scripts: compileSDB.sh, copyjar.sh, runstudentdb.sh, rundemodb.sh.

2) 	Run the script to compile SimpleDB and the studentClient and demoClient files by executing './compileSDB.sh' in your terminal.

3)	Next, run copyjar.sh with './copyjar.sh'. This makes a jar and copies it to the demoClient/simpledb and studentClient/simpledb folders.

4a)	If you want to run the studentdb, run runstudentdb.sh with './runstudentdb.sh'. Open a new terminal tab and switch to the studentClient/simpledb folder with 'cd studentClient/simpledb'.
	
4b)	If you want to run the demodb, run rundemodb.sh with './rundemodb.sh'. Open a new terminal tab and switch to the demoClient/simpledb folder with 'cd demoClient/simpledb'.

5a)	If you are running the studentdb for the first time, run 'java -cp simpledb.jar:. CreateStudentDB'. This will populate the studentdb and you can run 'java -cp simpledb.jar:. SQLInterpreter' to get the SQL interface.

5b)	If you are running the demo for the first time, run 'java -cp simpledb.jar:. CreateDemoDB'. This will populate the demodb and you can run 'java -cp simpledb.jar:. SQLInterpreter' to get the SQL interface. For the demodb, we have provided some sample queries in the demoqueries.txt file to run.