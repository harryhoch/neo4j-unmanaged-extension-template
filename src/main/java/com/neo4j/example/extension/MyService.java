package com.neo4j.example.extension;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
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
        }
        return Response.ok().entity(objectMapper.writeValueAsString(movieTitles)).build();
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
