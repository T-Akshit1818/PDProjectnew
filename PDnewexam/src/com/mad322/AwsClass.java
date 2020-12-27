package com.mad322;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.json.JSONArray;
import org.json.JSONObject;

import com.mad322.*;



@Path("/aws")
public class AwsClass {
	

	
	Connection con = null;
	Statement stmt = null;
	ResultSet rs = null;

	PreparedStatement preparedStatement = null;

	JSONObject mainObj = new JSONObject();
	JSONArray jsonArray = new JSONArray();
	JSONObject childObj = new JSONObject();

    //update employee 1st query by Akshit
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
    @Path("/updateEmp/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response updateEmployee(@PathParam("id")int id,Employee1 employee)
	{
		MysqlCon connection= new MysqlCon();
		con= connection.getConnection();
		Status status =Status.OK;
		try {
			String query ="UPDATE `midterm`.`employee` SET `EMP_ID` = ?, `END_DATE` = ?, `FIRST_NAME` = ?, `LAST_NAME` = ?,`START_DATE` = ?,`TITLE` = ?,`ASSIGNED_BRANCH_ID` = ?,`DEPT_ID` = ?,`SUPERIOR_EMP_ID` = ? WHERE `EMP_ID` = "+id;
		
			
			String query1="UPDATE `midterm`.`employee` SET `EMP_ID` = ?, `END_DATE` = ?, `FIRST_NAME` = ?, `LAST_NAME` = ?,`START_DATE` = ?,`TITLE` = ?,`ASSIGNED_BRANCH_ID` = ?,`DEPT_ID` = ?,`SUPERIOR_EMP_ID` = ? WHERE `EMP_ID` = "+id;

			preparedStatement = con.prepareStatement(query);
			
			preparedStatement.setInt(1,employee.geteMP_ID());
			preparedStatement.setString(2, employee.geteND_DATE());		
			preparedStatement.setString(3, employee.getfIRST_NAME());
			preparedStatement.setString(4, employee.getlAST_NAME());
			preparedStatement.setString(5, employee.getsTART_DATE());
			preparedStatement.setString(6, employee.gettITLE());
			preparedStatement.setInt(7,employee.getaSSIGNED_BRANCH_ID());
			preparedStatement.setInt(8, employee.getdEPT_ID());
			preparedStatement.setInt(9,employee.getsUPERIOR_EMP_ID());
			
			
int rowCount = preparedStatement.executeUpdate();
    		
    		if (rowCount > 0) 
    		{
    		status=Status.OK;
    		mainObj.accumulate("status", status);
    		mainObj.accumulate("Message","Data successfully updated !");
    		
    		}
    		else
    		{
    			status=Status.NOT_MODIFIED;
    			mainObj.accumulate("status", status);
    			mainObj.accumulate("Message","Something Went Wrong");
    						
    		}
    		
    	}catch(SQLException e) {
    		e.printStackTrace();
    		status=Status.NOT_MODIFIED;
    		mainObj.accumulate("status", status);
    		mainObj.accumulate("Message","Something Went Wrong");
    	}
    	
    	return Response.status(status).entity(mainObj.toString()).build();
    }
	
   //2nd method post by Akshit
	@POST
	@Path("/createEmp")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createEmp(Employee1 employee) {
		MysqlCon connection = new MysqlCon();

		con = connection.getConnection();

		try {

			// '' Single quotes
			// ` Grave accent

			String query = "INSERT INTO `midterm`.`employee`(`EMP_ID`,`END_DATE`,`FIRST_NAME`,`LAST_NAME`,`START_DATE`,`TITLE`,`ASSIGNED_BRANCH_ID`,`DEPT_ID`,`SUPERIOR_EMP_ID`)"
					+ "VALUES(?,?,?,?,?,?,?,?,?)";

			preparedStatement = con.prepareStatement(query);
			preparedStatement.setInt(1,employee.geteMP_ID());
			preparedStatement.setString(2, employee.geteND_DATE());		
			preparedStatement.setString(3, employee.getfIRST_NAME());
			preparedStatement.setString(4, employee.getlAST_NAME());
			preparedStatement.setString(5, employee.getsTART_DATE());
			preparedStatement.setString(6, employee.gettITLE());
			preparedStatement.setInt(7,employee.getaSSIGNED_BRANCH_ID());
			preparedStatement.setInt(8, employee.getdEPT_ID());
			preparedStatement.setInt(9,employee.getsUPERIOR_EMP_ID());
						int rowCount = preparedStatement.executeUpdate();

			if (rowCount > 0) {
				System.out.println("Record inserted Successfully! : " + rowCount);

				mainObj.accumulate("Status", 201);
				mainObj.accumulate("Message", "Record Successfully added!");
			} else {
				mainObj.accumulate("Status", 500);
				mainObj.accumulate("Message", "Something went wrong!");
			}

		} catch (SQLException e) {

			mainObj.accumulate("Status", 500);
			mainObj.accumulate("Message", e.getMessage());
		} finally {
			try {
				con.close();
				preparedStatement.close();
			} catch (SQLException e) {
				System.out.println("Finally SQL Exception : " + e.getMessage());
			}
		}

		return Response.status(201).entity(mainObj.toString()).build();

	}


	
}