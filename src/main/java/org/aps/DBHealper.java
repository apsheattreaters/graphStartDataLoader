package org.aps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBHealper {
	static final String driver = "com.mysql.jdbc.Driver";
	   static final String db = "jdbc:mysql://localhost:3306/timetempdata"; 
	   static final String user = "root";
	   static final String pass = "root";
	   
	   
	 public Connection getConnection() { 
    	 Connection connection = null;
        try{
            Class.forName(driver); 
            connection = DriverManager.getConnection(db, user, pass);               
        } 
        catch (SQLException e)  {
           e.printStackTrace();
        }
        catch(ClassNotFoundException e){
        	e.printStackTrace();
        }
		return connection;   
    } 


public void closeConnection(Connection connection) { 
	try
    { if(connection != null)
            connection.close();
    
    } catch (Exception e) {
    	e.printStackTrace();
    } 
}



}
