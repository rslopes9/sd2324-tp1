package tukano.api.rest;

import java.util.List;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import tukano.api.Short;

@Path(RestShorts.PATH)
public interface RestShorts {
	String PATH = "/shorts";
	
	String USER_ID = "userId";
	String USER_ID1 = "userId1";
	String USER_ID2 = "userId2";
	String SHORT_ID = "shortId";
	
	String PWD = "pwd";
	String FEED = "/feed";

	String LIKES = "/likes";
	String SHORTS = "/shorts";
	String FOLLOWERS = "/followers";
	
	@POST
	@Path("/{" + USER_ID + "}")
	@Produces(MediaType.APPLICATION_JSON)
	Short createShort(@PathParam(USER_ID) String userId, @QueryParam(PWD) String password);

	@DELETE
	@Path("/{" + SHORT_ID + "}")
	void deleteShort(@PathParam(SHORT_ID) String shortId, @QueryParam(PWD) String password);

	@GET
	@Path("/{" + SHORT_ID + "}" )
	@Produces(MediaType.APPLICATION_JSON)
	Short getShort(@PathParam(SHORT_ID) String shortId);

	@GET
	@Path("/{" + USER_ID + "}" + SHORTS )
	@Produces(MediaType.APPLICATION_JSON)
	List<String> getShorts(@PathParam(USER_ID) String userId);

	@POST
	@Path("/{" + USER_ID1 + "}/{" + USER_ID2 + "}" + FOLLOWERS )
	@Consumes(MediaType.APPLICATION_JSON)
	void follow(@PathParam(USER_ID1) String userId1, @PathParam(USER_ID2) String userId2, boolean isFollowing, @QueryParam(PWD) String password);

	@GET
	@Path("/{" + USER_ID + "}" + FOLLOWERS )
	@Produces(MediaType.APPLICATION_JSON)
	List<String> followers(@PathParam(USER_ID) String userId, @QueryParam(PWD) String password);

	@POST
	@Path("/{" + SHORT_ID + "}/{" + USER_ID + "}" + LIKES )
	@Consumes(MediaType.APPLICATION_JSON)
	void like(@PathParam(SHORT_ID) String shortId, @PathParam(USER_ID) String userId, boolean isLiked,  @QueryParam(PWD) String password);

	@GET
	@Path("/{" + SHORT_ID + "}" + LIKES )
	@Produces(MediaType.APPLICATION_JSON)
	List<String> likes(@PathParam(SHORT_ID) String shortId, @QueryParam(PWD) String password);

	@GET
	@Path("/{" + USER_ID + "}" + FEED )
	@Produces(MediaType.APPLICATION_JSON)
	List<String> getFeed( @PathParam(USER_ID) String userId, @QueryParam(PWD) String password);
}
