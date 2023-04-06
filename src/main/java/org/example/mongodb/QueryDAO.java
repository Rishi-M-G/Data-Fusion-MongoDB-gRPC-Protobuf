package org.example.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;

public class QueryDAO {
	
	public static void main(String[] args) throws InterruptedException
	{
		//***** CONNECTION STRING FOR MONGODB *****
		ConnectionString connectionString = new ConnectionString("mongodb+srv://rishikrishnan:Gopalakrishnan8*@cluster0.3jlwexc.mongodb.net/?retryWrites=true&w=majority");
		MongoClientSettings settings = MongoClientSettings.builder()
		        .applyConnectionString(connectionString)
		        .build();
		MongoClient mongoClient = MongoClients.create(settings);
		MongoDatabase database = mongoClient.getDatabase("MyDatabase");
		Thread.sleep(1000);
		
		//Query One
		//query_one(database);
		
		//Query Two
		//query_two(database);
		
		//Query Three
		//query_three(database);
		
		//Query Four
		query_four(database); 
		
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
	
	public static void query_one(MongoDatabase database) throws InterruptedException {
		//***** QUERY ONE *****
		//get collection
		MongoCollection<Document> collection = database.getCollection("SampleEduCostStat");
		
		//get input from users
		Scanner input = new Scanner(System.in);
		
		System.out.println("Enter year parameter:");
		int year = input.nextInt();
		
		input.nextLine();
		
		System.out.println("Enter state parameter");
		String state = input.nextLine();
		
		System.out.println("Enter type parameter");
		String type = input.nextLine();
		
		System.out.println("Enter length parameter");
		String length = input.nextLine();
		
		System.out.println("Enter expense parameter");
		String expense = input.nextLine();
		
		
		
		Bson query_1 = Filters.and(
				Filters.eq("Year", year),
				Filters.eq("State", state),
				Filters.eq("Type", type),
				Filters.eq("Length", length),
				Filters.eq("Expense", expense)
				);
		
		Bson projection = Projections.fields(Projections.excludeId(),Projections.include("Value"));
		
		//Creating a collection for Query 1
		MongoCollection<Document> query_collection = database.getCollection("SampleEduCostStatQueryOne");
		
		FindIterable<Document> docs = collection.find(query_1).projection(projection);
		
		for(Document doc : docs)
		{
			Object Value = doc.get("Value");
			Document query_result = new Document()
					.append("Value",Value);
					
			query_collection.insertOne(query_result);
		}
	}
	
	public static void query_two(MongoDatabase database) throws InterruptedException {
		//***** QUERY TWO *****
		
		//MongoDB command line example
		//db.SampleEduCostStat.aggregate([{ $match: {Type:"Private",Year:2013,Length:"4-year"}},{$project:{State:1,Value:1,_id:0}},{$group:{_id:"$State",Total_Value:{$sum:"$Value"}}},{$sort:{Total_Value:-1}},{$limit:5}])
		
		//get collection
		MongoCollection<Document> collection = database.getCollection("SampleEduCostStat");
		
		//get input from users
		Scanner input = new Scanner(System.in);
		
		System.out.println("Enter year parameter:");
		int year = input.nextInt();
		
		input.nextLine();
		
		System.out.println("Enter type parameter");
		String type = input.nextLine();
		
		System.out.println("Enter length parameter");
		String length = input.nextLine();
		
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
				new Document("$sort",new Document("Overall Expense",-1)),
				new Document("$limit",5)
				));
		
		//save results to a new collection
		List<Document> resultDocs = new ArrayList<>();
		for(Document doc:result) {
			resultDocs.add(doc);
		}
		
		//Creating a collection for Query 2
		MongoCollection<Document> query_collection = database.getCollection("SampleEduCostStatQueryTwo");
		
		//Insert the results in the collection
		query_collection.insertMany(resultDocs);
		
	}
	
	public static void query_three(MongoDatabase database) throws InterruptedException {
		//***** QUERY THREE *****
		
		//MongoDB command line example
		//db.SampleEduCostStat.aggregate([{ $match: {Type:"Private",Year:2013,Length:"4-year"}},{$project:{State:1,Value:1,_id:0}},{$group:{_id:"$State",Total_Value:{$sum:"$Value"}}},{$sort:{Total_Value:1}},{$limit:5}])
		
		//get collection
		MongoCollection<Document> collection = database.getCollection("SampleEduCostStat");
		
		//get input from users
		Scanner input = new Scanner(System.in);
		
		System.out.println("Enter year parameter:");
		int year = input.nextInt();
		
		input.nextLine();
		
		System.out.println("Enter type parameter");
		String type = input.nextLine();
		
		System.out.println("Enter length parameter");
		String length = input.nextLine();
		
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
		MongoCollection<Document> query_collection = database.getCollection("SampleEduCostStatQueryThree");
		
		//Insert the results in the collection
		query_collection.insertMany(resultDocs);
		
	}
	
	
	
	
	
	public static void query_four(MongoDatabase database) throws InterruptedException {
		//***** QUERY FOUR *****
		
		//MongoDB command line example
		//db.SampleEduCostStat.aggregate([{$match:{Type:"Private",Length:"4-year",Year:{$gte:2019}}},{$sort:{State:1,Year:-1}},{$group:{_id:"$State",Base_Value:{$first:"$Value"},Current_Values:{$push:"$Value"},Latest_Year:{$first:"$Year"}}},{$project:{State:"$_id",Growth_Rate_3_Years:{$let:{vars:{baseValue:{$arrayElemAt:["$Current_Values",0]},currentValue:{$arrayElemAt:["$Current_Values",2]}},in:{$cond:{if:{$eq:["$Latest_Year",2019]},then:0,else:{$multiply:[{$divide:[{$subtract:["$$currentValue","$$baseValue"]},"$$baseValue"]},100]}}}}}}},{$sort:{Growth_Rate_3_Years:-1}},{$limit:5}]);
		
		//get collection
		MongoCollection<Document> collection = database.getCollection("SampleEduCostStat");
		
		//get input from users
		Scanner input = new Scanner(System.in);
		
		System.out.println("Enter type parameter");
		String type = input.nextLine();
		
		System.out.println("Enter length parameter");
		String length = input.nextLine();
		
		System.out.println("Enter the range : 1.Past 1 year\n 2.Past 3 years\n 3.Past 5 Years");
		int switch_var = input.nextInt();
		input.nextLine();
		
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
			MongoCollection<Document> query_collection1 = database.getCollection("SampleEduCostStatQueryFour");
			
			//Insert the results in the collection
			query_collection1.insertMany(resultDocs1);
			break;
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
			MongoCollection<Document> query_collection2 = database.getCollection("SampleEduCostStatQueryFour");
			
			//Insert the results in the collection
			query_collection2.insertMany(resultDocs2);
			break;
		case 3:
			//pipeline for 3 years range from current year ( 2020 - 2018)
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
			MongoCollection<Document> query_collection3 = database.getCollection("SampleEduCostStatQueryFour");
			
			//Insert the results in the collection
			query_collection3.insertMany(resultDocs3);
			break;
		}
        
	}
        
	public static void query_five(MongoDatabase database) throws InterruptedException {
		//***** QUERY FIVE *****
		
		//MongoDB command line example
		//db.SampleEduCostStat.aggregate([{$match:{Year:2013,Type:"Private",Length:"4-year",State:{$in:["Washington","Oregan","California","Nevada","Idaho","Utah","Montana","Wyoming","Colorado","Alaska","Hawaii"]}}},{$group:{_id:"West",total_value:{$sum:"$Value"}}}])
		
		//get collection
		MongoCollection<Document> collection = database.getCollection("SampleEduCostStat");
		
		
		//get input from users
		Scanner input = new Scanner(System.in);
		
		System.out.println("Enter year parameter:");
		int year = input.nextInt();
		
		input.nextLine();
		
		
		System.out.println("Enter type parameter");
		String type = input.nextLine();
		
		System.out.println("Enter length parameter");
		String length = input.nextLine();
		
		
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

		// print the results
//		for (Document result : results) {
//		    System.out.println(result.toJson());
//		}
		
		List<Document> resultDocs = new ArrayList<>();
		for(Document doc:results) {
			resultDocs.add(doc);
		}
		
		//Creating a collection for Query 3
		MongoCollection<Document> query_collection = database.getCollection("SampleEduCostStatQueryFive");
		
		//Insert the results in the collection
		query_collection.insertMany(resultDocs);
	
	}
	
}
