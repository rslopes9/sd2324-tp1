package tukano.api.java;


import java.util.List;

import tukano.api.Short;

/**
 * 
 * Interface for the Shorts service.
 * 
 * This service allows existing users to create or delete short videos. 
 * Users can follow other users to gain access to their short videos.
 * User can add or remove likes to short videos.
 * 
 * @author smd
 *
 */
public interface Shorts {
	
	String NAME = "shorts";
	
	/**
	 * Creates a new short, generating its unique identifier. 
	 * The result short will include the blob storage location where the media should be uploaded.
	 * 
	 * @param userId - the owner of the new short
	 * @param password - the password of owner of the new short
	 * @return (OK, Short) if the short was created;
	 * NOT FOUND, if the owner of the short does not exist;
	 * FORBIDDEN, if the password is not correct;
	 * BAD_REQUEST, otherwise.
	 */
	Result<Short> createShort(String userId, String password);

	/**
	 * Deletes a given Short.
	 * 
	 * @param shortId the unique identifier of the short to be deleted
	 * @return (OK,void), 
	 * 	NOT_FOUND if shortId does not match an existing short
	 * 	FORBIDDEN, if the password is not correct;
	 */
	Result<Void> deleteShort(String shortId, String password);
	
	
	/**
	 * Retrieves a given Short.
	 * 
	 * @param shortId the unique identifier of the short to be deleted
	 * @return (OK,Short), 
	 * 	NOT_FOUND if shortId does not match an existing short
	 */
	Result<Short> getShort(String shortId);
	
	
	/**
	 * Retrieves the list of identifiers of the shorts created by the given user, with its total likes count updated.
	 * 
	 * @param userId the user that owns the requested shorts
	 * @return (OK, List<String>|empty list) or NOT_FOUND if the user does not exist
	 */
	Result<List<String>> getShorts( String userId );
	
	/**
	 * Causes a user to follow the shorts of another user.
	 * 
	 * @param userId1     the user that will follow or cease to follow the
	 *                    followed user
	 * @param userId2     the followed user
	 * @param isFollowing flag that indicates the desired end status of the
	 *                    operation
	 * @param password 	  the password of the follower
	 * @return (OK,), 
	 * 	NOT_FOUND if any of the users does not exist
	 *  FORBIDDEN if the password is incorrect
	 */
	Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password);	

	/**
	 * Retrieves the lost of users following a given user
	 * @param userId - the followed user
	 * @param password - the password of the followed user
	 * @return (OK, List<String>|empty list) the list of users that follow another user, or empty if the user has no followers 
	 * NOT_FOUND if the user does not exists
	 * FORBIDDEN if the password is incorrect
	 */
	Result<List<String>> followers(String userId, String password);
	/**
	 * Adds or removes a like to a short
	 * 
	 * @param shortId  - the identifier of the post
	 * @param userId  - the identifier of the user
	 * @param isLiked - a flag with true to add a like, false to remove the like
	 * @return (OK,void) if the like was added/removed; 
	 * 	NOT_FOUND if either the short or the like being removed does not exist, 
	 *  CONFLICT if the like already exists.
	 *  FORBIDDEN if the password of the user is incorrect
	 *  BAD_REQUEST, otherwise
	 */
	Result<Void> like(String shortId, String userId, boolean isLiked, String password);
	

	/**
	 * Returns all the likes of a given short
	 * 
	 * @param shortId the identifier of the short
	 * @param password the password of the owner of the short
	 * @return (OK,Boolean), 
	 * NOT_FOUND if there is no Short with the given shortId
	 * FORBIDDEN if the password is incorrect
	 */
	Result<List<String>> likes(String shortId, String password);


	/**
	 * Returns the feed of the user, sorted by age. The feed is the list of shorts made by
	 * the users followed by the user.
	 * 
	 * @param userId user of the requested feed
	 * @param password the password of the user
	 * @return (OK,List<PostId>|empty list)
	 * 	NOT_FOUND if the user does not exists
	 *  FORBIDDEN if the password is incorrect
	 */
	Result<List<String>> getFeed(String userId, String password);
}
