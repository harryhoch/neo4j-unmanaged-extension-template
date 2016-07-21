package com.neo4j.example.extension;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.*;

@Path("/service")
public class MyService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    enum Labels implements Label {
        Person
    }

    enum RelTypes implements RelationshipType {
        KNOWS
    }

    @GET
    @Path("/helloworld")
    public String helloWorld() {
        return "Hello World!";
    }

    @GET
    @Path("/friendsCypher/{name}")
    public Response getFriendsCypher(@PathParam("name") String name, @Context GraphDatabaseService db) throws IOException {
        Result result = db.execute("MATCH (p:Person)-[:KNOWS]-(friend) WHERE p.name = {n} RETURN friend.name",
                Collections.<String, Object>singletonMap("n", name));
        List<String> friendNames = new ArrayList<>();
        for (Map<String, Object> item : Iterators.asIterable(result)) {
            friendNames.add((String) item.get("friend.name"));
        }
        return Response.ok().entity(objectMapper.writeValueAsString(friendNames)).build();
    }

    @GET
    @Path("/actedCypher/{name}")
    public Response getMoviesCypher(@PathParam("name") String name, @Context GraphDatabaseService db) throws IOException {
        Result result = db.execute("MATCH (p:Person)-[:ACTED_IN]-(movie) WHERE p.name = {n} RETURN movie.title",
                Collections.<String, Object>singletonMap("n", name));
        List<String> movieTitles = new ArrayList<>();
        for (Map<String, Object> item : Iterators.asIterable(result)) {
            movieTitles.add((String) item.get("movie.title"));
	    System.err.println(item.get("movie.title"));
        }
        return Response.ok().entity(objectMapper.writeValueAsString(movieTitles)).build();
    }


    @GET
    @Path("/actedCypherJson/{name}")
    public Response getMoviesCypherJson(@PathParam("name") String name, @Context GraphDatabaseService db) throws IOException {
        Result result = db.execute("MATCH (p:Person)-[:ACTED_IN]-(movie) WHERE p.name = {n} RETURN movie.title",
                Collections.<String, Object>singletonMap("n", name));
        List<String> movieTitles = new ArrayList<>();
        for (Map<String, Object> item : Iterators.asIterable(result)) {
            movieTitles.add((String) item.get("movie.title"));
        }
	final ByteArrayOutputStream out = new ByteArrayOutputStream();
	objectMapper.writeValue(out,movieTitles);
	String res = new String(out.toByteArray());
        return Response.ok().entity(res).build();
    }

    @GET
    @Path("/actedCypherStreaming/{name}")

    
    public Response getMoviesCypherStreaming(@PathParam("name") String name, @Context GraphDatabaseService db) throws IOException {


	final Map<String,Object> params= Collections.<String, Object>singletonMap("n", name);

        StreamingOutput stream = new StreamingOutput() {

		@Override
		public void write (OutputStream os) throws IOException, WebApplicationException {
		    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os,JsonEncoding.UTF8);
		    jg.writeStartObject();
		    jg.writeFieldName( "films" );
		    jg.writeStartArray();

		    try ( Transaction tx = db.beginTx();
			  Result result = db.execute( actedQuery(), params ) ) {
			while ( result.hasNext() )   {
			    Map<String,Object> row = result.next();
			    jg.writeString((String) row.get("movie.title"));
			}
			tx.success();
		    }
		    jg.writeEndArray();
		    jg.writeEndObject();
		    jg.flush();
		    jg.close();
		}
	    };
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }



    private String actedQuery() {
	    return "MATCH (p:Person)-[:ACTED_IN]-(movie) WHERE p.name = {n} RETURN movie.title";
    }

    

    @GET
    @Path("/friendsJava/{name}")
    public Response getFriendsJava(@PathParam("name") String name, @Context GraphDatabaseService db) throws IOException {

        List<String> friendNames = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {
            Node person = Iterators.single(db.findNodes(Labels.Person, "name", name));

            for (Relationship knowsRel : person.getRelationships(RelTypes.KNOWS, Direction.BOTH)) {
                Node friend = knowsRel.getOtherNode(person);
                friendNames.add((String) friend.getProperty("name"));
            }
            tx.success();
        }
        return Response.ok().entity(objectMapper.writeValueAsString(friendNames)).build();
    }
}
