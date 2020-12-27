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

	@POST
	@Path("/createBranch")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createbranch(Branch branch) {
		MysqlCon connection = new MysqlCon();

		con = connection.getConnection();

		try {



			String query = "INSERT INTO `midterm`.`branch`(`BRANCH_ID,ADDRESS, CITY,NAME, STATE,ZIP_CODE`)"
					+ "VALUES(?,?,?,?,?,?,?,?,?)";

			preparedStatement = con.prepareStatement(query);


			preparedStatement.setInt(1, branch.getBRANCH_ID());
			preparedStatement.setString(2, branch.getADDRESS());
			preparedStatement.setString(3, branch.getCITY());
			preparedStatement.setString(4, branch.getNAME());
			preparedStatement.setString(5, branch.getSTATE());
			preparedStatement.setString(6, branch.getZIP_CODE());


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
	@GET
	@Path("/getAccount/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAccount(@PathParam("id") String id) {
		MysqlCon connection = new MysqlCon();

		con = connection.getConnection();

		try {
			stmt = con.createStatement();

			rs = stmt.executeQuery("Select * from account where pending_balance<"+id);

			while (rs.next()) {
				childObj = new JSONObject();

				childObj.accumulate("accountid", rs.getString("account_id"));
				childObj.accumulate("availablebalance", rs.getString("avail_balance"));
				childObj.accumulate("closedate", rs.getDate("close_date"));
				childObj.accumulate("lastactivity", rs.getDate("last_activity_date"));
				childObj.accumulate("opendate",rs.getDate("open_date"));
				childObj.accumulate("pendingbalance",rs.getString("pending_balance"));
				childObj.accumulate("status",rs.getString("status"));
				childObj.accumulate("custid",rs.getString("cust_id"));
				childObj.accumulate("openbranchid",rs.getString("open_branch_id"));
				childObj.accumulate("openempid",rs.getString("open_emp_id"));
				childObj.accumulate("productcd",rs.getString("product_cd"));
				jsonArray.put(childObj);
			}

			mainObj.put("Account", jsonArray);
		} catch (SQLException e) {
			System.out.println("SQL Exception : " + e.getMessage());
		} finally {
			try {
				con.close();
				stmt.close();
				rs.close();
			} catch (SQLException e) {
				System.out.println("Finally Block SQL Exception : " + e.getMessage());
			}
		}

		return Response.status(200).entity(mainObj.toString()).build();

	}
	@DELETE
	@Path("/delAccount/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteAccount(@PathParam("id") String id)
	{
		MysqlCon connection= new MysqlCon();
		con= connection.getConnection();
		Status status= Status.OK;
		try
		{
			String query="DELETE  FROM `acc_transaction` WHERE account_id = "+id;
			stmt= con.createStatement();
			stmt.executeUpdate(query);

			int rowCount = stmt.executeUpdate(query);
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


		}catch(SQLException e)
		{
			e.printStackTrace();
			status=Status.NOT_MODIFIED;
			mainObj.accumulate("status",status);
			mainObj.accumulate("Message","Something Went Wrong");
		}


		return Response.status(status).entity(mainObj.toString()).build();
	}
	@GET
	@Path("/getAccountTrans/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAccountTrans(@PathParam("id") String id) {
		MysqlCon connection = new MysqlCon();

		con = connection.getConnection();

		try {
			stmt = con.createStatement();

			rs = stmt.executeQuery("Select 	FUNDS_AVAIL_DATE,TXN_DATE from acc_transaction where  ACCOUNT_ID<"+id);

			while (rs.next()) {
				childObj = new JSONObject();

				childObj.accumulate("TXN_DATE", rs.getString("TXN_DATE"));
				childObj.accumulate("FUNDS_AVAIL_DATE", rs.getString("FUNDS_AVAIL_DATE"));
				;
				jsonArray.put(childObj);
			}

			mainObj.put("Account", jsonArray);
		} catch (SQLException e) {
			System.out.println("SQL Exception : " + e.getMessage());
		} finally {
			try {
				con.close();
				stmt.close();
				rs.close();
			} catch (SQLException e) {
				System.out.println("Finally Block SQL Exception : " + e.getMessage());
			}
		}

		return Response.status(200).entity(mainObj.toString()).build();

	}

@DELETE
	@Path("/delemp/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response selectEmployee(@PathParam("id") String id)
	{
		MysqlCon connection= new MysqlCon();
		con= connection.getConnection();
		Status status= Status.OK;
		try
		{
			String query="DELETE FROM `midterm`.`employee` WHERE `EMP_ID` = "+id;
			stmt= con.createStatement();		
			stmt.executeUpdate(query);
			
			int rowCount = stmt.executeUpdate(query);
			if (rowCount > 0) 
    		{
    		status=Status.OK;
    		mainObj.accumulate("status", status);
    		mainObj.accumulate("Message","Data successfully Deleted !");
    		
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

@PUT
	 @Consumes(MediaType.APPLICATION_JSON)
	 @Path("/Cust/{id}")
	 @Produces(MediaType.APPLICATION_JSON)
	 public Response updateCust(@PathParam("id")int id,Customer cust)
	 {
	 	MysqlCon connection= new MysqlCon();
	 	con= connection.getConnection();
	 	Status status =Status.OK;
	 	try {
	 		String query ="UPDATE `midterm`.`customer` SET `CUST_ID` =?,`ADDRESS` =?,`CITY` =?,`CUST_TYPE_CD` =?, `FED_ID` =?,`POSTAL_CODE` =?,`STATE` =? WHERE `CUST_ID` ="+id;
	 		
	 
	 		preparedStatement = con.prepareStatement(query);   		
	 		   		
	 		preparedStatement.setInt(1, cust.getCUST_ID());
	 		preparedStatement.setString(2, cust.getADDRESS());
	 		preparedStatement.setString(3,cust.getCITY());
	 		preparedStatement.setString(4,cust.getCUST_TYPE_CD());
	 		preparedStatement.setString(5,cust.getFED_ID());
	 		preparedStatement.setString(6, cust.getPOSTAL_CODE());
	 		preparedStatement.setString(7, cust.getSTATE());
	 		
	 		

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

}