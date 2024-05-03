package tukano.impl.api.rest;

import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import tukano.api.rest.RestBlobs;

@Path(RestBlobs.PATH)
public interface RestExtendedBlobs extends RestBlobs {

	String TOKEN = "token";
	String USER_ID = "userId";
	String BLOBS = "blobs";
	
	@DELETE
	@Path("/{" + USER_ID + "}/" + BLOBS)
	void deleteAllBlobs(@PathParam(USER_ID) String userId, @QueryParam(TOKEN) String password );		
}
