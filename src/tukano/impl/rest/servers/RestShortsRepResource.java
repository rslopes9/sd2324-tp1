package tukano.impl.rest.servers;


import java.util.List;
import java.util.logging.Logger;

import com.google.gson.Gson;

import jakarta.inject.Singleton;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import tukano.api.Short;
import tukano.api.User;
import tukano.api.java.Blobs;
import tukano.api.java.Result;
import tukano.api.rest.RestShorts;
import tukano.impl.api.java.ExtendedShorts;
import tukano.impl.api.rest.RestExtendedShorts;
import tukano.impl.java.servers.JavaRepShorts;
import tukano.impl.kafka.SyncPoint;
import utils.DB;

import static java.lang.String.format;
import static tukano.api.java.Result.error;
import static tukano.api.java.Result.errorOrResult;
import static tukano.api.java.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.java.Result.ErrorCode.CONFLICT;


@Provider
@Singleton
public class RestShortsRepResource extends RestResource implements RestExtendedShorts {
	private static Logger Log = Logger.getLogger(RestShortsRepResource.class.getName());
	
	final JavaRepShorts impl;
	private final ShortsEventService eventService;
	
    private Long version;
    
    private final String CREATE_SHORT = "createShort";
    private final String GET_SHORT = "getShort";
    private final String DELETE_SHORT = "deleteShort";
    private final String LIKE_SHORT = "likeShort";
    private final String FOLLOW_SHORT = "followShort";
    private final String DELETE_ALL_SHORTS = "deleteAllShort";
    
    private SyncPoint<Result<?>> syncPoint = SyncPoint.getInstance();;
    
    Gson gson = new Gson();


	public RestShortsRepResource() {
		this.impl = new JavaRepShorts();
		this.eventService = new ShortsEventService("kafka:9092");
	}
	@Override
	public Short getShort(String shortId) {
		return super.resultOrThrow( impl.getShort(shortId));
		//return null;
	}	
	
	@Override
	public Short getShort(@HeaderParam(RestShorts.HEADER_VERSION) Long version, String shortId) {
		
		Log.info("Received getShort; Version: "+ version +")");
		
		if( shortId == null )
			throw new IllegalArgumentException("ShortId can't be null: ");
		
		String value = shortId;
		
		Result<Short> result = eventService.publish(version,GET_SHORT,value);
		
		
        Short shortObj = super.resultOrThrow(result);
		throw new WebApplicationException(Response.status(200).
				header(RestShorts.HEADER_VERSION, version).
	            encoding(MediaType.APPLICATION_JSON).entity(shortObj).build());
	    /*
	    Log.info("Received getShort; Version: "+ version +")");
	    
	    if(shortId == null)
	        throw new IllegalArgumentException("ShortId can't be null");
	    
	    String value = shortId;
	    
	    Result<Short> result = eventService.publish(version, GET_SHORT, value);
	    
	    if (!result.isOK()) {
	        throw new WebApplicationException("Short not found", Response.Status.NOT_FOUND);
	    }
	    
	    Short shortObj = super.resultOrThrow(result);
	    return shortObj;
	            */
	}
	
	@Override
	public Short createShort(String userId, String password) {
		//Deve verificar pelo menos as condições que não são dependentes do estado do servidor shorts antes de propagar a operação - isto garante que a execução da operação nas réplicas não depende de terceiros que possam levar a que falhem numas réplicas e sucedam noutras.
		
		// o timeStamp tem de ser enviado daqui para ser igual em todos os servidores, ou seja depois do servidor criar o short dá set ao time stamp para o mesmo que todos 
		
	    Result<User> userResult = impl.okUser(userId, password);
	    if (!userResult.isOK()) {
	        // If user verification fails, throw an exception or handle it according to your error handling policy
	        throw new IllegalArgumentException("Authentication failed: " + userResult.error().name());
	    }
	    
		String value = gson.toJson(new Short("" , userId , "", System.currentTimeMillis(), 0));
		Result<Short> result = eventService.publish(version,CREATE_SHORT,value);
		
		return super.resultOrThrow(result);
	}
	

	@Override
	public void deleteShort(String shortId, String password) {	
		
		Result<Short> shortResult = impl.getShort(shortId);
		if (!shortResult.isOK()) {
			// If user verification fails, throw an exception or handle it according to your error handling policy
			throw new IllegalArgumentException("No short found: " + shortResult.error().name());
		}
	
		Result<User> userResult = impl.okUser(shortResult.value().getOwnerId(), password);
		if (!userResult.isOK()) {
			// If user verification fails, throw an exception or handle it according to your error handling policy
			throw new IllegalArgumentException("Authentication failed: " + userResult.error().name());
		}
		
		String value = shortId;
		
		
		eventService.publish(version,DELETE_SHORT,value);
	}
	
	@Override
	public void like(String shortId, String userId, boolean isLiked, String password) {

		String isLikedString;
		if(isLiked) {
			isLikedString = "1";
		}
		else {
			isLikedString = "0";
		}

		String value = shortId + ":" + isLikedString + ":" + userId + ":" + password;

		
		eventService.publish(version,LIKE_SHORT,value);
		
		
		
	}
	
	@Override
	public void follow(String userId1, String userId2, boolean isFollowing, String password) {
		var query = format("SELECT f.followee FROM Following f WHERE f.follower = '%s'", userId1);

		List<String> list = DB.sql(query, String.class);
		System.out.println("weyf" + list);
		if(list.contains(userId2) && isFollowing) {
			System.out.println("weydf" + list);
			super.resultOrThrow(Result.error(CONFLICT));
		}
		
		String isFollowingString;
		if(isFollowing) {
			isFollowingString = "1";
		}
		else {
			isFollowingString = "0";
		}

		String value = userId1 + ":" + userId2 + ":" + isFollowingString + ":" + password;

		System.out.println("erta3: "+ value);
		eventService.publish(version,FOLLOW_SHORT,value);
	}
	
	@Override
	public void deleteAllShorts(String userId, String password, String token) {
		
		String value = userId + ":" + password + ":" + token;
		eventService.publish(version,DELETE_ALL_SHORTS,value);
		
		
	}
	

	
	
	


	@Override
	public List<String> getShorts(String userId) {
		return super.resultOrThrow( impl.getShorts(userId));
	}

	@Override
	public List<String> followers(String userId, String password) {
		return super.resultOrThrow( impl.followers(userId, password));
	}


	@Override
	public List<String> likes(String shortId, String password) {
		return super.resultOrThrow( impl.likes(shortId, password));
	}

	@Override
	public List<String> getFeed(String userId, String password) {
		return super.resultOrThrow( impl.getFeed(userId, password));
	}


}
