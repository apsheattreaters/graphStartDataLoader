package org.aps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Scanner;





public class DateChange {
	

	static final String driver = "com.mysql.cj.jdbc.Driver";
	static final String db = "jdbc:mysql://localhost:3306/timetempdata"; 
	static final String user = "root";
	static final String pass = "root";
	static final String folderName= "E://Temperature Data";
	//old path \\\\Controlpanel\\C\\Temperature  Data
	static final String insertQury="INSERT INTO timetempdata.data "
			+ "(time, pv1, sv1, pv2, sv2, pv3, sv3, pv4, sv4, pv5, sv5,"
			+ " pv6, sv6, pv7, sv7, pv8, sv8, pv9, sv9, pv10, sv10,monitor) "
			+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
	static final String updateQury="UPDATE timetempdata.data set "
			+ "pv1= ? , sv1= ? , pv2= ? , sv2= ? , pv3= ? , sv3= ? , pv4= ? , sv4= ? , pv5= ? , sv5= ? ,"
			+ " pv6= ? , sv6= ? , pv7= ? , sv7= ? , pv8= ? , sv8= ? , pv9= ? , sv9= ? , pv10= ? , sv10= ?  "
			+ "where monitor= ?  and time = ?";
	static final String getQury="select pv1 from timetempdata.data where monitor= ?  and time = ?";

	static File folder= new File(folderName);
	static String monitor=null;
	

	public static void main(String Args[]) throws IOException
	{
		System.out.println("Reading from "+folderName);
		System.out.println("Time format must be hh:mm:ss and file format must be dd-mm-yyyy");

		try {
			listFilesForFolder(folder);
		} catch (SQLException e) { 
			e.printStackTrace(); 
		}	
		finally
		{
			Scanner scan=new Scanner(System.in); 
			System.out.print("\nPress any key to continue . . . ");
			scan.nextLine();
			scan.close();
		}
	}


	public static void pushData(String fileName) throws IOException, SQLException
	{  
		System.out.println("\\"+monitor+"\\"+fileName);
		BufferedReader br = new BufferedReader(new FileReader(folder+"\\"+monitor+"\\"+fileName));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();  
			while (line != null) { 
				if(line.contains("Time") || line.trim().length()==0){
					line = br.readLine(); 
				}
				else{
					tokenizeLine(line,fileName);
					line = br.readLine();  
				}
			} 
			sb.toString();

		} finally {
			br.close();
		}

	}

	private static void tokenizeLine(String line,String fileName) throws SQLException {
		String arr [] = line.split("\t");  
		for(int i=0;i<arr.length;i++)
		{ 
			if(arr[0].indexOf(":")==2 || arr[0].indexOf(":")==1)
			{
				arr[0]=makeDate(arr[0], fileName);
				System.out.println(arr[0]);
			}
			if(arr[i].equals("Empty"))
				arr[i]="0"; 
		}

		if(arr.length==21)
		{
			putInDb(arr); 
		}
	}

	private static void putInDb(String[] arr) throws SQLException {  
		Connection connection = null;
		try{ 
			Class.forName(driver); 
			connection = DriverManager.getConnection(db, user, pass);      
			PreparedStatement  stmatInsert = connection.prepareStatement(insertQury); 
			for (int i = 0; i < arr.length; i++) {  
				stmatInsert.setString(i+1, arr[i]); 
			}
			stmatInsert.setString(22,monitor); 
			System.out.println("Begin");
			System.out.println(arr[0]);
			stmatInsert.execute(); 
			//System.out.println("Data added for time : "+arr[0]+" monitor : "+monitor);

		}
		catch(SQLException e)
		{
			if(e.getMessage().startsWith("Duplicate entry"))
			{ 

				PreparedStatement  stmat = connection.prepareStatement(getQury);   
				stmat.setString(2,arr[0]);
				stmat.setString(1,monitor); 
				ResultSet getResult = stmat.executeQuery(); 
				if(getResult.next()){
					System.out.println(arr[0]);
					PreparedStatement  stmatUpdate = connection.prepareStatement(updateQury);  
					for (int i = 1; i < arr.length; i++) {  
						stmatUpdate.setString(i, arr[i]); 
					}
					stmatUpdate.setString(arr.length+1,arr[0]);
					stmatUpdate.setString(arr.length,monitor); 
					int flag = stmatUpdate.executeUpdate(); 
					if(flag==1){ 
						System.out.println(arr[0]);
						//System.out.println("Data Changed for time : "+arr[0]+" monitor : "+monitor);
					}
				}

			}
			else
			{
				System.out.println(e.getMessage());
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		finally
		{
			connection.close(); 
		}


	}

	private static String makeDate(String time,String fileName) {
		try {
			String[] strTime = time.split(":");
			fileName = fileName.replace(".txt", "");
			String[] strDate = fileName.split("-");
			strDate[2] = strDate[2].substring(0, 4);
			Calendar cal = new GregorianCalendar();
			cal.set(Calendar.YEAR, Integer.parseInt(strDate[2]));
			cal.set(Calendar.MONTH, Integer.parseInt(strDate[1]) - 1);
			cal.set(Calendar.DATE, Integer.parseInt(strDate[0]));
			cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(strTime[0]));
			cal.set(Calendar.MINUTE, Integer.parseInt(strTime[1]));
			cal.set(Calendar.SECOND, Integer.parseInt(strTime[2]));
			cal.set(Calendar.MILLISECOND, 0);

			//System.out.println("Cal "+cal.getTime());
			//System.out.println(strTime[0]+""+strTime[1]+""+strTime[2]+"    =    "+cal.getTimeInMillis());
			System.out.println(cal.getTimeInMillis());
			cal.getTime();
			return "" + cal.getTimeInMillis();
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return "";
	}

	public static void listFilesForFolder(final File folder) throws IOException, SQLException {
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) { 
				monitor=fileEntry.getName();
				System.out.println("Reading "+monitor);
				listFilesForFolder(fileEntry);
			} else {
				if(fileEntry.getName().endsWith(".txt"))
				{
					pushData(fileEntry.getName());
					fileRename(fileEntry);
				}
			}
		}
	}

	static void fileRename(File fileEntry) throws IOException
	{ 
		File file2 = new File(fileEntry.getPath()+".done_"+new GregorianCalendar().getTimeInMillis());  
		if(!fileEntry.renameTo(

				
				file2)) 
		{  
			System.out.println("Cannot rename"+fileEntry.getPath());   
		}
	}


}

