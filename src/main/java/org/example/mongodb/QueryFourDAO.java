package org.example.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

public class QueryFourDAO {
	
	public static void main(String[] args) throws InterruptedException
	{
//		//***** CONNECTION STRING FOR MONGODB *****
//		ConnectionString connectionString = new ConnectionString("mongodb+srv://rishikrishnan:Gopalakrishnan8*@cluster0.3jlwexc.mongodb.net/?retryWrites=true&w=majority");
//		MongoClientSettings settings = MongoClientSettings.builder()
//		        .applyConnectionString(connectionString)
//		        .build();
//		MongoClient mongoClient = MongoClients.create(settings);
//		MongoDatabase database = mongoClient.getDatabase("MyDatabase");
//		Thread.sleep(1000);
		
		
		//Query Four
		//query_four(database);
		
		
	}
	
	public static void databaseDetails(MongoDatabase database,MongoClient mongoClient) {
		//***** DATABASE DETAILS *****
		MongoIterable<String> listDataBases = mongoClient.listDatabaseNames();
		System.out.println("List of Databases");
		for(String Database : listDataBases)
		{
			database = mongoClient.getDatabase(Database);
			System.out.println(Database);
		}
		System.out.println("List of Collections in Each Database");
		for(String Database : listDataBases)
		{
			database = mongoClient.getDatabase(Database);
			System.out.println(Database+" : ");
			System.out.println();
			MongoIterable<String> listCollections = database.listCollectionNames();
			for(String col : listCollections)
			{
				System.out.println(col);
			}
		}
	}
	
	public List<Document> query_four(String type,String length,int switch_var) throws InterruptedException {
		//***** CONNECTION STRING FOR MONGODB *****
		ConnectionString connectionString = new ConnectionString("mongodb+srv://rishikrishnan:Gopalakrishnan8*@cluster0.3jlwexc.mongodb.net/?retryWrites=true&w=majority");
		MongoClientSettings settings = MongoClientSettings.builder()
		        .applyConnectionString(connectionString)
		        .build();
		MongoClient mongoClient = MongoClients.create(settings);
		MongoDatabase database = mongoClient.getDatabase("MyDatabase");
		Thread.sleep(1000);
		//***** QUERY FOUR *****

		//MongoDB command line example
		//db.SampleEduCostStat.aggregate([{$match:{Type:"Private",Length:"4-year",Year:{$gte:2019}}},{$sort:{State:1,Year:-1}},{$group:{_id:"$State",Base_Value:{$first:"$Value"},Current_Values:{$push:"$Value"},Latest_Year:{$first:"$Year"}}},{$project:{State:"$_id",Growth_Rate_3_Years:{$let:{vars:{baseValue:{$arrayElemAt:["$Current_Values",0]},currentValue:{$arrayElemAt:["$Current_Values",2]}},in:{$cond:{if:{$eq:["$Latest_Year",2019]},then:0,else:{$multiply:[{$divide:[{$subtract:["$$currentValue","$$baseValue"]},"$$baseValue"]},100]}}}}}}},{$sort:{Growth_Rate_3_Years:-1}},{$limit:5}]);
		
		//get collection
		MongoCollection<Document> collection = database.getCollection("SampleEduCostStat");
		
		//Building aggregation pipeline
		
		switch(switch_var)
		{
		case 1:
			//pipeline for 1-year year range from current year ( 2020 - 2021)
			List<Document> pipeline1 = Arrays.asList(
			        new Document("$match", new Document("Type", "Private").append("Length", "4-year").append("Year", new Document("$gte", 2020))),
			        new Document("$sort", new Document("State", 1).append("Year", -1)),
			        new Document("$group", new Document("_id", "$State").append("Base_Value", new Document("$first", "$Value")).append("Current_Values", new Document("$push", "$Value")).append("Latest_Year", new Document("$first", "$Year"))),
			        new Document("$project", new Document("State", "$_id").append("Growth_Rate_1_Year", new Document("$let", new Document("vars", new Document("baseValue", new Document("$arrayElemAt", Arrays.asList("$Current_Values", 0))).append("currentValue", new Document("$arrayElemAt", Arrays.asList("$Current_Values", 1)))).append("in", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$Latest_Year", 2020)), 0, new Document("$multiply", Arrays.asList(new Document("$divide", Arrays.asList(new Document("$subtract", Arrays.asList("$$currentValue", "$$baseValue")), "$$baseValue")), 100)))))))),
			        new Document("$sort", new Document("Growth_Rate_1_Year", -1)),
			        new Document("$limit", 5));

			AggregateIterable<Document> results1 = collection.aggregate(pipeline1);
			List<Document> resultDocs1 = new ArrayList<>();
			for (Document doc : results1) {
				System.out.println(doc);
			}
			//Creating a collection for Query 4
			MongoCollection<Document> query_collection1 = database.getCollection("EduCostStatQueryFour");
			
			query_collection1.createIndex(Indexes.ascending("State","Growth_Rate_1_Year"),new IndexOptions().unique(true));
			//Insert the results in the collection
			try {
				query_collection1.insertMany(resultDocs1);
			}catch (MongoWriteException e)
			{
				if(e.getError().getCode() == 11000 )
				{
					//duplicate key error
					System.out.println("Duplicate Document Found");
				}else
				{
					throw e;
				}
			}
			return resultDocs1;
		case 2:
			//pipeline for 3 years range from current year ( 2020 - 2018)
			List<Document> pipeline2 = Arrays.asList(
			        new Document("$match", new Document("Type", "Private").append("Length", "4-year").append("Year", new Document("$gte", 2018))),
			        new Document("$sort", new Document("State", 1).append("Year", -1)),
			        new Document("$group", new Document("_id", "$State").append("Base_Value", new Document("$first", "$Value")).append("Current_Values", new Document("$push", "$Value")).append("Latest_Year", new Document("$first", "$Year"))),
			        new Document("$project", new Document("State", "$_id").append("Growth_Rate_3_Years", new Document("$let", new Document("vars", new Document("baseValue", new Document("$arrayElemAt", Arrays.asList("$Current_Values", 0))).append("currentValue", new Document("$arrayElemAt", Arrays.asList("$Current_Values", 3)))).append("in", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$Latest_Year", 2018)), 0, new Document("$multiply", Arrays.asList(new Document("$divide", Arrays.asList(new Document("$subtract", Arrays.asList("$$currentValue", "$$baseValue")), "$$baseValue")), 100)))))))),
			        new Document("$sort", new Document("Growth_Rate_3_Years", -1)),
			        new Document("$limit", 5));

			AggregateIterable<Document> results2 = collection.aggregate(pipeline2);
			List<Document> resultDocs2 = new ArrayList<>();
			for (Document doc : results2) {
				System.out.println(doc);
			}
			//Creating a collection for Query 4
			MongoCollection<Document> query_collection2 = database.getCollection("EduCostStatQueryFour");
			
			query_collection2.createIndex(Indexes.ascending("State","Growth_Rate_3_Years"),new IndexOptions().unique(true));
			//Insert the results in the collection
			try {
				query_collection2.insertMany(resultDocs2);
			}catch (MongoWriteException e)
			{
				if(e.getError().getCode() == 11000 )
				{
					//duplicate key error
					System.out.println("Duplicate Document Found");
				}else
				{
					throw e;
				}
			}
			
			return resultDocs2;
		case 3:
			//pipeline for 5 years range from current year ( 2020 - 2016)
			List<Document> pipeline3 = Arrays.asList(
			        new Document("$match", new Document("Type", "Private").append("Length", "4-year").append("Year", new Document("$gte", 2016))),
			        new Document("$sort", new Document("State", 1).append("Year", -1)),
			        new Document("$group", new Document("_id", "$State").append("Base_Value", new Document("$first", "$Value")).append("Current_Values", new Document("$push", "$Value")).append("Latest_Year", new Document("$first", "$Year"))),
			        new Document("$project", new Document("State", "$_id").append("Growth_Rate_5_Years", new Document("$let", new Document("vars", new Document("baseValue", new Document("$arrayElemAt", Arrays.asList("$Current_Values", 0))).append("currentValue", new Document("$arrayElemAt", Arrays.asList("$Current_Values", 5)))).append("in", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$Latest_Year", 2016)), 0, new Document("$multiply", Arrays.asList(new Document("$divide", Arrays.asList(new Document("$subtract", Arrays.asList("$$currentValue", "$$baseValue")), "$$baseValue")), 100)))))))),
			        new Document("$sort", new Document("Growth_Rate_5_Years", -1)),
			        new Document("$limit", 5));

			AggregateIterable<Document> results3 = collection.aggregate(pipeline3);
			List<Document> resultDocs3 = new ArrayList<>();
			for (Document doc : results3) {
				System.out.println(doc);
			}
			//Creating a collection for Query 4
			MongoCollection<Document> query_collection3 = database.getCollection("EduCostStatQueryFour");
			
			query_collection3.createIndex(Indexes.ascending("State","Growth_Rate_5_Years"),new IndexOptions().unique(true));
			//Insert the results in the collection
			try {
				query_collection3.insertMany(resultDocs3);
			}catch (MongoWriteException e)
			{
				if(e.getError().getCode() == 11000 )
				{
					//duplicate key error
					System.out.println("Duplicate Document Found");
				}else
				{
					throw e;
				}
			}
			
			return resultDocs3;
		}
		return null;
	}
	
	
	

}
