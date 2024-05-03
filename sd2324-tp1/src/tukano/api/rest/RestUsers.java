package tukano.api.rest;

import java.util.List;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import tukano.api.User;

@Path(RestUsers.PATH)
public interface RestUsers {

	String PATH = "/users";

	String PWD = "pwd";
	String QUERY = "query";
	String USER_ID = "userId";
	
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	String createUser(User user);
	
	
	@GET
	@Path("/{" + USER_ID+ "}")
	@Produces(MediaType.APPLICATION_JSON)
	User getUser(@PathParam(USER_ID) String userId, @QueryParam( PWD ) String pwd);
	
	
	@PUT
	@Path("/{" + USER_ID+ "}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	User updateUser(@PathParam( USER_ID ) String userId, @QueryParam( PWD ) String pwd, User user);
	
	
	@DELETE
	@Path("/{" + USER_ID+ "}")
	@Produces(MediaType.APPLICATION_JSON)
	User deleteUser(@PathParam(USER_ID) String userId, @QueryParam(PWD) String pwd);
	
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	List<User> searchUsers(@QueryParam(QUERY) String pattern);	
}
