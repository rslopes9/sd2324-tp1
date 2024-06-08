package tukano.impl.api.rest;

import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import tukano.api.rest.RestBlobs;

@Path(RestBlobs.PATH)
public interface RestExtendedBlobs extends RestBlobs {

	String TOKEN = "token";
	String BLOBS = "blobs";
	String USER_ID = "userId";

	@DELETE
	@Path("/{" + BLOB_ID + "}")
	void delete(@PathParam(BLOB_ID) String blobId, @QueryParam(TOKEN) String token );		

	
	@DELETE
	@Path("/{" + USER_ID + "}/" + BLOBS)
	void deleteAllBlobs(@PathParam(USER_ID) String userId, @QueryParam(TOKEN) String token );		
}
