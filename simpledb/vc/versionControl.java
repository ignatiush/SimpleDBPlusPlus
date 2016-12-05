package simpledb.vc;

import simpledb.file.FileMgr;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardCopyOption.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.lang.Object;

/**
 * Created by iggy on 11/30/16.
 */
public class versionControl implements Serializable{

    private static final long serialVersionUID = 1L;

    private String dbDirectory;
    private versionControlNode current;
    private int commitID;
    private HashMap<String, Collection<versionControlNode>> allCommitsByMessage;
    private HashMap<Integer, versionControlNode> allCommitsByID;

    public versionControl() {
        current = null;
        commitID = 0;
        allCommitsByMessage = new HashMap<String, Collection<versionControlNode>>();
        allCommitsByID = new HashMap<Integer, versionControlNode>();
    }

    public void init(FileMgr fm){
        dbDirectory = fm.getDbDirectory().getAbsolutePath();
        File vcObj = new File(dbDirectory + "/.vcObj/");
        if (!vcObj.exists()){
            vcObj.mkdir();
        }
    }

    public void commit(String message){
        versionControlNode newCommit = new versionControlNode(commitID, message, current);
        this.current = newCommit;
        // add myCommit to the allCommitsByMessage HashMap
        if (!allCommitsByMessage.containsKey(message)) {
            allCommitsByMessage.put(message, new ArrayList<versionControlNode>());
        }
        allCommitsByMessage.get(message).add(current);
        // add myCommit to the allCommitsByID HashMap
        allCommitsByID.put(commitID, newCommit);
        File commitFile = new File(dbDirectory + "/.vcObj/Commit-" + commitID + "/");
        try{
            commitFile.mkdir();
        } catch(Exception e){
            e.printStackTrace();
        }
        File dbDirFile = new File(this.dbDirectory);
        File[] listOfFiles = dbDirFile.listFiles();
        for (File f:listOfFiles){
            if (f.isFile()){
                if ((f.getName().endsWith(".tbl")) || (f.getName().endsWith(".log"))){
                    File target = new File(dbDirectory + "/.vcObj/Commit-" + commitID + "/" + f.getName());
                    try{
                        copyFile(f, target);
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                    newCommit.addFile(target);
                }
            }
        }
        this.commitID++;
    }

    public void checkout(String id) {
        try {
            Integer checkoutID = Integer.parseInt(id);
            if (!allCommitsByID.containsKey(checkoutID))
                System.out.println("No commit with that id exists.");
            else {
                versionControlNode checkoutNode = allCommitsByID.get(checkoutID);
                for (File f: checkoutNode.files){
                    //System.out.println(f.getAbsolutePath());
                    File target = new File(dbDirectory + "/" + f.getName());
                    try{
                        copyFile(f, target);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            System.out.println("Please check that you have enter a valid number for the commitID.");
        }
    }

        /**
         * Method for copying a file to a destination file.
         *
         * @param sourceFile - file to be copied
         * @param destFile - destination to copy the file to
         * @throws IOException
         */

    @SuppressWarnings("resource")
    private static void copyFile(File sourceFile, File destFile) throws IOException {
        if(!destFile.exists()) {
            destFile.createNewFile();
        }
        System.out.println("source is " + sourceFile.getName() + " at " + sourceFile.getAbsolutePath());
        System.out.println("dest is " + destFile.getName() + " at " + destFile.getAbsolutePath());

        FileChannel source = null;
        FileChannel destination = null;

        try {
            source = new FileInputStream(sourceFile).getChannel();
            destination = new FileOutputStream(destFile, false).getChannel();
            destination.transferFrom(source, 0, source.size());
        }
        finally {
            if(source != null) {
                source.close();
            }
            if(destination != null) {
                destination.close();
            }
        }
    }

    public static class versionControlNode implements Serializable{

        private static final long serialVersionUID = 2L;

        versionControlNode myPrev;
        ArrayList<File> files;
        Integer id;
        String message;
        Date myDate;
        Timestamp myTime;

        public versionControlNode(int commitID, String commitMessage, versionControlNode prev){
            myPrev = prev;
            id = commitID;
            message = commitMessage;
            files = new ArrayList<File>();
            myDate = new Date();
            myTime = new Timestamp(myDate.getTime());
        }

        public void addFile(File f){
            this.files.add(f);
        }
    }
}
