package org.example.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;

public class QueryOneDAO {
	
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
		
		int year = 0;
		String state = null,type = null,length = null,expense = null;
		//Query One
		//query_one(year,state,type,length,expense);
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
	
	public List<Document> query_one(int year,String state,String type,String length,String expense) throws InterruptedException {
		//***** CONNECTION STRING FOR MONGODB *****
				ConnectionString connectionString = new ConnectionString("mongodb+srv://rishikrishnan:Gopalakrishnan8*@cluster0.3jlwexc.mongodb.net/?retryWrites=true&w=majority");
				MongoClientSettings settings = MongoClientSettings.builder()
				        .applyConnectionString(connectionString)
				        .build();
				MongoClient mongoClient = MongoClients.create(settings);
				MongoDatabase database = mongoClient.getDatabase("MyDatabase");
				Thread.sleep(1000);
		
		//***** QUERY ONE *****
		//get collection
		MongoCollection<Document> collection = database.getCollection("SampleEduCostStat");
		
		Bson query_1 = Filters.and(
				Filters.eq("Year", year),
				Filters.eq("State", state),
				Filters.eq("Type", type),
				Filters.eq("Length", length),
				Filters.eq("Expense", expense)
				);
		
		Bson projection = Projections.fields(Projections.excludeId(),Projections.include("Value"));
		
		//Creating a collection for Query 1
		MongoCollection<Document> query_collection = database.getCollection("EduCostStatQueryOne");
		
		query_collection.createIndex(Indexes.ascending("Value"),new IndexOptions().unique(true));
		
		FindIterable<Document> docs = collection.find(query_1).projection(projection);
		
		List<Document> resultDocs = new ArrayList<>();
		
		for(Document doc : docs)
		{
			Object Value = doc.get("Value");
			Document query_result = new Document()
					.append("Value",Value);
			resultDocs.add(doc);
			try {
				query_collection.insertOne(query_result);
			}catch (MongoWriteException e)
			{
				if(e.getError().getCode() == 11000 )
				{
					//duplicate key error
					System.out.println("Duplicate Document Found: "+query_result);
				}else
				{
					throw e;
				}
			}
					
			
		}
		//System.out.println(docs);
		return resultDocs;
		
		
	}
}
	
