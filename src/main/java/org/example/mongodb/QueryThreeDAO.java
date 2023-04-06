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

public class QueryThreeDAO {
	
	public static void main(String[] args) throws InterruptedException
	{
		
		
		
		
		//Query Three
//		query_three(database);
		
		
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
	
	public List<Document> query_three(int year,String type,String length) throws InterruptedException {
		
		//***** CONNECTION STRING FOR MONGODB *****
		ConnectionString connectionString = new ConnectionString("mongodb+srv://rishikrishnan:Gopalakrishnan8*@cluster0.3jlwexc.mongodb.net/?retryWrites=true&w=majority");
		MongoClientSettings settings = MongoClientSettings.builder()
		        .applyConnectionString(connectionString)
		        .build();
		MongoClient mongoClient = MongoClients.create(settings);
		MongoDatabase database = mongoClient.getDatabase("MyDatabase");
				Thread.sleep(1000);
		//***** QUERY THREE *****
		
		//MongoDB command line example
		//db.SampleEduCostStat.aggregate([{ $match: {Type:"Private",Year:2013,Length:"4-year"}},{$project:{State:1,Value:1,_id:0}},{$group:{_id:"$State",Total_Value:{$sum:"$Value"}}},{$sort:{Total_Value:1}},{$limit:5}])
		
		//get collection
		MongoCollection<Document> collection = database.getCollection("SampleEduCostStat");
		
		//Building aggregation pipeline
		AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
				new Document("$match",new Document ("Type",type)
						.append("Year",year)
						.append("Length",length)),
				new Document("$project",new Document("State",1)
						.append("Value",1)
						.append("_id", 0)),
				new Document("$group",new Document("_id","$State")
						.append("Overall Expense", new Document("$sum","$Value"))),
				new Document("$sort",new Document("Overall Expense",1)),
				new Document("$limit",5)
				));
		
		//save results to a new collection
		List<Document> resultDocs = new ArrayList<>();
		for(Document doc:result) {
			resultDocs.add(doc);
		}
		
		//Creating a collection for Query 3
		MongoCollection<Document> query_collection = database.getCollection("EduCostStatQueryThree");
		
		query_collection.createIndex(Indexes.ascending("_id","Overall Expense"),new IndexOptions().unique(true));
		
		//Insert the results in the collection
		try {
			query_collection.insertMany(resultDocs);
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
		
		
		return resultDocs;
		
	}
	
	

}
