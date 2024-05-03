package tukano.impl.api.rest;

import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import tukano.api.rest.RestShorts;

@Path(RestShorts.PATH)
public interface RestExtendedShorts extends RestShorts {

	String TOKEN = "token";

	
	@DELETE
	@Path("/{" + USER_ID + "}" + SHORTS)
	void deleteAllShorts(@PathParam(USER_ID) String userId, @QueryParam(PWD) String password, @QueryParam(TOKEN) String token);
		
}
