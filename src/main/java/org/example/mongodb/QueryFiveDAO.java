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

public class QueryFiveDAO {
	
	public static void main(String[] args) throws InterruptedException
	{
		
		
		//Query One
		//query_one(database);
		
		//Query Two
		//query_two(database);
		
		//Query Three
		//query_three(database);
		
		//Query Four
		//query_four(database);
		
		//Query Five
		//query_five(database);
		
		//databaseDetails(database,mongoClient);
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
	
	public List<Document> query_five(int year,String length,String type) throws InterruptedException {
		
		//***** CONNECTION STRING FOR MONGODB *****
		ConnectionString connectionString = new ConnectionString("mongodb+srv://rishikrishnan:Gopalakrishnan8*@cluster0.3jlwexc.mongodb.net/?retryWrites=true&w=majority");
		MongoClientSettings settings = MongoClientSettings.builder()
		        .applyConnectionString(connectionString)
		        .build();
		MongoClient mongoClient = MongoClients.create(settings);
		MongoDatabase database = mongoClient.getDatabase("MyDatabase");
		Thread.sleep(1000);
		
		//***** QUERY FIVE *****
		
		//MongoDB command line example
		//db.SampleEduCostStat.aggregate([{$match:{Year:2013,Type:"Private",Length:"4-year",State:{$in:["Washington","Oregan","California","Nevada","Idaho","Utah","Montana","Wyoming","Colorado","Alaska","Hawaii"]}}},{$group:{_id:"West",total_value:{$sum:"$Value"}}}])
		
		//get collection
		MongoCollection<Document> collection = database.getCollection("SampleEduCostStat");
		
		List<Document> results = new ArrayList<>();
		
		//WEST STATES
		AggregateIterable<Document> westResult = collection.aggregate(Arrays.asList(
			    new Document("$match", new Document("Year", year)
			        .append("Type", type)
			        .append("Length", length)
			        .append("State", new Document("$in", Arrays.asList("Washington", "Oregan", "California", "Nevada", "Idaho", "Utah", "Montana", "Wyoming", "Colorado", "Alaska", "Hawaii")))),
			    new Document("$group", new Document("_id", "West")
			        .append("total_value", new Document("$sum", "$Value")))
			));
		results.add(westResult.first());
		
		
		//SOUTH WEST STATES
		AggregateIterable<Document> southWestResult = collection.aggregate(Arrays.asList(
		    new Document("$match", new Document("Year", 2013)
		        .append("Type", "Private")
		        .append("Length", "4-year")
		        .append("State", new Document("$in", Arrays.asList("Arizona","New Mexico","Texas","Oklahoma")))),
		    new Document("$group", new Document("_id", "South West")
		        .append("total_value", new Document("$sum", "$Value")))
		));
		results.add(southWestResult.first());
		
		
		//MID WEST STATES
		AggregateIterable<Document> midWestResults = collection.aggregate(Arrays.asList(
		    new Document("$match", new Document("Year", 2013)
		        .append("Type", "Private")
		        .append("Length", "4-year")
		        .append("State", new Document("$in", Arrays.asList("North Dakota", "South Dakota", "Nebraska", "Kansas", "Minnesota", "Iowa", "Missouri", "Wisconsin", "Illinois", "Michigan", "Indiana", "Ohio")))),
		    new Document("$group", new Document("_id", "Mid West")
		        .append("total_value", new Document("$sum", "$Value")))
		));
		results.add(midWestResults.first());
		
		
		//SOUTH EAST STATES
		AggregateIterable<Document> southEastResults = collection.aggregate(Arrays.asList(
			    new Document("$match", new Document("Year", 2013)
			        .append("Type", "Private")
			        .append("Length", "4-year")
			        .append("State", new Document("$in", Arrays.asList("Arkansas","Louisiana","Mississippi","Alabama","Tennessee","Kentucky","West Virginia","Georgia","South Carolina","North Carolina","Florida","Virginia","District of Columbia","Delaware")))),
			    new Document("$group", new Document("_id", "South East")
			        .append("total_value", new Document("$sum", "$Value")))
			));
		results.add(southEastResults.first());
		
		//SOUTH EAST STATES
		AggregateIterable<Document> northEastResults = collection.aggregate(Arrays.asList(
			    new Document("$match", new Document("Year", 2013)
			        .append("Type", "Private")
			        .append("Length", "4-year")
			        .append("State", new Document("$in", Arrays.asList("Maine","Pennsylvania","Maryland","New York","Vermont","New Hampshire","Massachusetts","Rhode Island","Connecticut","New Jersey")))),
			    new Document("$group", new Document("_id", "North East")
			        .append("total_value", new Document("$sum", "$Value")))
			));
		results.add(northEastResults.first());

		// sort the results in descending order based on total_value
		results.sort((doc1, doc2) -> ((Integer) doc2.getInteger("total_value")).compareTo(doc1.getInteger("total_value")));
		
		List<Document> resultDocs = new ArrayList<>();
		for(Document doc:results) {
			resultDocs.add(doc);
		}
		
		//Creating a collection for Query 3
		MongoCollection<Document> query_collection = database.getCollection("EduCostStatQueryFive");
		
		query_collection.createIndex(Indexes.ascending("_id","total_value"),new IndexOptions().unique(true));
		
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
