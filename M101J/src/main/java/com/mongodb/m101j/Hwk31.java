package com.mongodb.m101j;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

public class Hwk31 {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
/*		
		//MongoClientOptions options = MongoClientOptions.builder().build();
		MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(100).build();
		MongoClient client = new MongoClient(new ServerAddress(), options);
		
		//MongoClient client = new MongoClient();
		//Or MongoClient client = new MongoClient(localhost);
		//Or MongoClient client = new MongoClient(localhost,port);
		//Or MongoClient client = new MongoClient(new ServerAddress("localhost", 27017));
		//Or MongoClient client = new MongoClient(asList(new ServerAddress("localhost", 27017)));
		//Or MongoClient client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		
		MongoDatabase db = client.getDatabase("grades").withReadPreference(ReadPreference.secondary());
		db.getCollection("grades");
		
		MongoCollection<Document> coll = db.getConnection("grades");
*/		
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("school");
        // DB db = client.getDB("database name"); >> DEPRECATED OLD WAY. DO NOT USE.
        MongoCollection<Document> collection = database.getCollection("students");
        
        // Sample JSON used in this sheet
        //{ "_id" : 65, "name" : "Gena Riccio", "scores" : [{ "type" : "exam", "score" : 67.58395308948619 }, { "type" : "quiz", "score" : 67.2413500951588 }, { "type" : "homework", "score" : 42.93471779899529 }, { "type" : "homework", "score" : 30.12776583664075 }] }
        //{ "_id" : 179, "name" : "Gena Riccio", "scores" : [{ "type" : "exam", "score" : 81.49070346172086 }, { "type" : "quiz", "score" : 23.12653402998139 }, { "type" : "homework", "score" : 96.54590960898932 }, { "type" : "homework", "score" : 2.928015597639488 }] }


        //@WorksButCommented - To drop a collection
        //collection.drop();
        
        //@WorksButCommented - General commands
        //mongoimport --drop -d students -c grades grades.hw2.json
        //db.grades.count()
        //db.grades.find().sort( { 'score' : -1 } ).skip( 100 ).limit( 1 )
        
        //@WorksButCommented - To list first element
        //System.out.println("Find one:");
        //Document first = collection.find().first();
        //printJson(first);        
        
        //@WorksButCommented - Projection and sorting
        /*{
        Bson projection = fields(include("_id", "scores"), excludeId());
        Bson sort = descending("scores.type");
        List<Document> all = collection.find().projection(projection).sort(sort).skip(20).limit(50)
        .into(new ArrayList<Document>());
		System.out.println("Printing:");
		for (Document cur : all) {
			printJson(cur);
		}
		System.out.println("Done");
        }*/

        //@WorksButCommented - Simple Query
        /*{
        BasicDBObject query = new BasicDBObject();
        query.put("name", new BasicDBObject("$eq", "Gisela Levin"));
        System.out.println("Printing:");
		for (Document doc : collection.find(query)) {
		    System.out.println(doc.toJson());
		}
		System.out.println("Done");
        }*/

        // Common Examples for filter 
        //Bson filter = and(eq("x", 0), gt("y", 10), lt("y", 90));
        //Bson filter = exists("scores.type", true);
        //Bson projection = fields(include("_id", "scores"), excludeId("name"));
        //Bson sort = descending("_id", "scores");
        
        //@WorksButCommented - Simple Query with projections and sort
        /*{
        BasicDBObject query = new BasicDBObject();
        query.put("name", new BasicDBObject("$eq", "Gena Riccio"));
        //com.mongodb.client.model.Projections
        Bson projection = Projections.fields(Projections.include("name", "scores"), Projections.excludeId());
        //com.mongodb.client.model.Sorts
        Bson sort = Sorts.descending("scores.type");
        //List<Document> all = collection.find().projection(projection).sort(sort).skip(20).limit(50)
        List<Document> all = collection.find(query).projection(projection).sort(sort)
        .into(new ArrayList<Document>());
        System.out.println("Printing:");
		for (Document doc : all) {
		    System.out.println(doc.toJson());
		}
		System.out.println("Done");
        }*/

        //@WorksButCommented - Update sample 
        /*{
            BasicDBObject query = new BasicDBObject();
            query.put("name", new BasicDBObject("$regex", "Gena Riccio"));
	        List<Document> all = collection.find(query).into(new ArrayList<Document>());
	        System.out.println("Printing before update:");
	        for (Document doc : all) {
				System.out.println(doc.toJson());
			    System.out.println("Id is " + doc.get("_id") + ", name is " + doc.get("name"));
			    BasicDBObject updateQuery = new BasicDBObject();
			    updateQuery.put("_id", doc.get("_id"));
			    BasicDBObject updateCommand = new BasicDBObject("$set", new BasicDBObject("name", doc.get("name").toString()+"."));
			    collection.updateOne(updateQuery, updateCommand);			    
			}
			//After update, for printing only
	        List<Document> allAfterUpdate = collection.find(query).into(new ArrayList<Document>());
		    System.out.println("Printing after update:");
			for (Document doc : allAfterUpdate) {
				System.out.println(doc.toJson());
			}
			System.out.println("");
        }*/

        // Exclude scores.type of not homework type
        {
	        BasicDBObject query = new BasicDBObject();
	        query.put("name", new BasicDBObject("$regex", "Gena Riccio"));
	        //com.mongodb.client.model.Projections
	        Bson projection = Projections.fields(Projections.include("name", "scores"));
	        //com.mongodb.client.model.Sorts
	        Bson sort = Sorts.descending("scores.type");
	    	{	
		        //List<Document> all = collection.find().projection(projection).sort(sort).skip(20).limit(50)
		        List<Document> all = collection.find(query).projection(projection).sort(sort)
		        .into(new ArrayList<Document>());
		        System.out.println("Printing:");
				for (Document doc : all) {
				    System.out.println(doc.toJson());
				}
				System.out.println("Done");
				System.out.println("");
	    	}
	    	{
	    		boolean bHomework=false;
	    		int homeworkCount=0;
	    		double minScore = 0;
	    		Document minScoreIndexDocument = null;
	    		double prevId;
	    		double _id;
	    		int i = -1;
	    		List<Document> all = collection.find().projection(projection)
	            //List<Document> all = collection.find(query).projection(projection).sort(sort)
	            .into(new ArrayList<Document>());
	            System.out.println("Processing:");
	    		for (Document doc : all) {
	    			System.out.println(doc.toJson());
	    			int id = ((Integer) doc.get("_id")).intValue() ;
	    			System.out.println("" + ++i + ", Id is " + doc.get("_id") + ", name is " + doc.get("name"));
	    		    //System.out.println(doc.toJson());
	            	
	    			//Get the scores size
	    			System.out.println("Get the scores size");
	    			    List scores = (ArrayList) doc.get("scores");
	    			    System.out.println("    scores size is " + scores.size());
	            	
	    			//For the scores, filter by homework only
	    			System.out.println("For the scores, filter by homework only");
	    			    
	    			    for(int j=0; j<scores.size();j++) {
	    			    	Document scoreDoc = (Document) scores.get(j);
	    			    	System.out.println("" + j + ", type is " + scoreDoc.get("type") + ", score is " + scoreDoc.get("score"));
	    			    	if("homework".equals(scoreDoc.get("type"))) {
	    			    	   System.out.println("homework Found");
	    			    		if (!bHomework  ||  ((Double)scoreDoc.get("score")).doubleValue()<minScore) {
	    			        	   minScoreIndexDocument = scoreDoc;
	    			        	   minScore = ((Double)scoreDoc.get("score")).doubleValue();
	    			        	   System.out.println("New min score assigned, " + minScore);
	    			           }
    			        	   bHomework = true;
    			        	   homeworkCount++;
	    			    	}
	    			    }
	    			
	    			//Find the index for homework with with minimum score
	    			System.out.println("Find the index for homework with with minimum score");
	    			System.out.println("minScoreIndex is " + (-1) + ", minScore is " + minScore);
	    			//Update by removing the homework at selected index
	    			System.out.println("Delete the homework at selected index");
	    			if (homeworkCount>1) {
	    				scores.remove(minScoreIndexDocument);
	    				System.out.println("Removed");
	    			}
	    			else {
	    				System.out.println("NOT Removed, homeworkCount is " + homeworkCount);
	    			}
	    			
				    BasicDBObject updateQuery = new BasicDBObject();
				    updateQuery.put("_id", doc.get("_id"));
				    BasicDBObject updateCommand = new BasicDBObject("$set", new BasicDBObject("scores", scores));
				    collection.updateOne(updateQuery, updateCommand);
				    System.out.println("Updated. New score size " + scores.size());
				    System.out.println("");
	    			
	    			bHomework=false;
	    			homeworkCount=0;
	    			minScoreIndexDocument = null;
				    minScore = 0.00;
	
	    			//if (i>10) break;
	    		}
	    		System.out.println("Done");	
	    	}
	    }
	}
}

