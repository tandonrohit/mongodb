package com.mongodb.m101j;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.m101j.util.Helpers.printJson;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

public class Hwk23 {

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
        MongoDatabase database = client.getDatabase("students");
        MongoCollection<Document> collection = database.getCollection("grades");

        //collection.drop();
        //mongoimport --drop -d students -c grades grades.hw2.json
        //db.grades.count()
        //db.grades.find().sort( { 'score' : -1 } ).skip( 100 ).limit( 1 )
        
        System.out.println("Find one:");
        //Document first = collection.find().first();
        //printJson(first);        

        //Bson filter = and(eq("x", 0), gt("y", 10), lt("y", 90));
        Bson projection = fields(include("student_id", "type", "score"), excludeId());
        Bson sort = descending("student_id", "score");

        /*List<Document> all = collection.find()
                .projection(projection)
                .sort(sort)
        //        .skip(20)
        //        .limit(50)
                .into(new ArrayList<Document>());
        System.out.println("Printing:");
		for (Document cur : all) {
			printJson(cur);
		}
		System.out.println("Done");
		*/
        
        int prevStudent_id = -1;
        int deleted = 0;
        System.out.println("Find all with iteration: ");
        MongoCursor<Document> cursor = collection.find().projection(projection).sort(sort).iterator();
        try {
        	Document cur = null;
        	Document previousDoc = null;
            while (cursor.hasNext()) {
                cur = cursor.next();
                System.out.println("Doc is " + cur);
                //cur.get(score);
                //String student_id = (String) cur.get("student_id") ;
                int student_id = ((Integer) cur.get("student_id")).intValue() ;
                System.out.println("student_id is " + student_id);
                String type = (String) cur.get("type") ;
                System.out.println("type is " + type);
                if (!type.equals("homework")) continue;

                double score = ((Double) cur.get("score")).doubleValue() ;
                System.out.println("score is " + score);
                printJson(cur);
                if (prevStudent_id== -1) {
                	System.out.println("Setting previous id for the first time ");
                }
                else if (prevStudent_id!=student_id) {
                	System.out.println("Deleting student_id " + student_id + " with score of " + score);
                	collection.deleteOne(previousDoc);
                	++deleted;
                	System.out.println("Deleted");
                }
                prevStudent_id = student_id;
                previousDoc = cur;
            }
            //Deleting the last one
            if(cur!=null && cursor.hasNext()==false) {
            	System.out.println("Deleting last record");
            	collection.deleteOne(previousDoc);
            	++deleted;
            	
            }
        } finally {
            cursor.close();
        }

        System.out.println("Count:");
        long count = collection.count();
        System.out.println(count);
        System.out.println("Deleted " + deleted);
	}

}
