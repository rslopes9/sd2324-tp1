package tukano.impl.java.servers;

import static java.lang.String.format;
import static tukano.api.java.Result.error;
import static tukano.api.java.Result.errorOrResult;
import static tukano.api.java.Result.errorOrValue;
import static tukano.api.java.Result.errorOrVoid;
import static tukano.api.java.Result.ok;
import static tukano.api.java.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.java.Result.ErrorCode.FORBIDDEN;
import static tukano.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.java.Result.ErrorCode.TIMEOUT;
import static tukano.impl.java.clients.Clients.BlobsClients;
import static tukano.impl.java.clients.Clients.UsersClients;
import static utils.DB.getOne;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import tukano.api.Short;
import tukano.api.User;
import tukano.api.java.Blobs;
import tukano.api.java.Result;
import tukano.impl.api.java.ExtendedShorts;
import tukano.impl.java.servers.data.Following;
import tukano.impl.java.servers.data.Likes;
import utils.DB;
import utils.Token;

public class JavaShorts implements ExtendedShorts {
	private static final String BLOB_COUNT = "*";

	private static Logger Log = Logger.getLogger(JavaShorts.class.getName());

	AtomicLong counter = new AtomicLong( totalShortsInDatabase() );
	
	private static final long USER_CACHE_EXPIRATION = 3000;
	private static final long SHORTS_CACHE_EXPIRATION = 3000;
	private static final long BLOBS_USAGE_CACHE_EXPIRATION = 10000;


	static record Credentials(String userId, String pwd) {
		static Credentials from(String userId, String pwd) {
			return new Credentials(userId, pwd);
		}
	}

	protected final LoadingCache<Credentials, Result<User>> usersCache = CacheBuilder.newBuilder()
			.expireAfterWrite(Duration.ofMillis(USER_CACHE_EXPIRATION)).removalListener((e) -> {
			}).build(new CacheLoader<>() {
				@Override
				public Result<User> load(Credentials u) throws Exception {
					var res = UsersClients.get().getUser(u.userId(), u.pwd());
					if (res.error() == TIMEOUT)
						return error(BAD_REQUEST);
					return res;
				}
			});
	
	protected final LoadingCache<String, Result<Short>> shortsCache = CacheBuilder.newBuilder()
			.expireAfterWrite(Duration.ofMillis(SHORTS_CACHE_EXPIRATION)).removalListener((e) -> {
			}).build(new CacheLoader<>() {
				@Override
				public Result<Short> load(String shortId) throws Exception {
					
					var query = format("SELECT count(*) FROM Likes l WHERE l.shortId = '%s'", shortId);
					var likes = DB.sql(query, Long.class);
					return errorOrValue( getOne(shortId, Short.class), shrt -> shrt.copyWith( likes.get(0) ) );
				}
			});
	
	protected final LoadingCache<String, Map<String,Long>> blobCountCache = CacheBuilder.newBuilder()
			.expireAfterWrite(Duration.ofMillis(BLOBS_USAGE_CACHE_EXPIRATION)).removalListener((e) -> {
			}).build(new CacheLoader<>() {
				@Override
				public Map<String,Long> load(String __) throws Exception {
					final var QUERY = "SELECT REGEXP_SUBSTRING(s.blobUrl, '^(\\w+:\\/\\/)?([^\\/]+)\\/([^\\/]+)') AS baseURI, count('*') AS usage From Short s GROUP BY baseURI";		
					var hits = DB.sql(QUERY, BlobServerCount.class);
					
					var candidates = hits.stream().collect( Collectors.toMap( BlobServerCount::baseURI, BlobServerCount::count));

					for( var uri : BlobsClients.all() )
						 candidates.putIfAbsent( uri.toString(), 0L);

					return candidates;

				}
			});
	
	@Override
	public Result<Short> createShort(String userId, String password) {
		Log.info(() -> format("createShort : userId = %s, pwd = %s\n", userId, password));

		return errorOrResult( okUser(userId, password), user -> {
			
			var shortId = format("%s-%d", userId, counter.incrementAndGet());
			var blobUrl = format("%s/%s/%s", getLeastLoadedBlobServerURI(), Blobs.NAME, shortId); 
			var shrt = new Short(shortId, userId, blobUrl);

			return DB.insertOne(shrt);
		});
	}

	@Override
	public Result<Short> getShort(String shortId) {
		Log.info(() -> format("getShort : shortId = %s\n", shortId));

		if( shortId == null )
			return error(BAD_REQUEST);

		return shortFromCache(shortId);
	}

	
	@Override
	public Result<Void> deleteShort(String shortId, String password) {
		Log.info(() -> format("deleteShort : shortId = %s, pwd = %s\n", shortId, password));
		
		return errorOrResult( getShort(shortId), shrt -> {
			
			return errorOrResult( okUser( shrt.getOwnerId(), password), user -> {
				return DB.transaction( hibernate -> {

					shortsCache.invalidate( shortId );
					hibernate.remove( shrt);
					
					var query = format("SELECT * FROM Likes l WHERE l.shortId = '%s'", shortId);
					hibernate.createNativeQuery( query, Likes.class).list().forEach( hibernate::remove);
					
					BlobsClients.get().delete(shrt.getBlobUrl(), Token.get() );
				});
			});	
		});
	}

	@Override
	public Result<List<String>> getShorts(String userId) {
		Log.info(() -> format("getShorts : userId = %s\n", userId));

		var query = format("SELECT s.shortId FROM Short s WHERE s.ownerId = '%s'", userId);
		return errorOrValue( okUser(userId), DB.sql( query, String.class));
	}

	@Override
	public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
		Log.info(() -> format("follow : userId1 = %s, userId2 = %s, isFollowing = %s, pwd = %s\n", userId1, userId2, isFollowing, password));
	
		
		return errorOrResult( okUser(userId1, password), user -> {
			var f = new Following(userId1, userId2);
			return errorOrVoid( okUser( userId2), isFollowing ? DB.insertOne( f ) : DB.deleteOne( f ));	
		});			
	}

	@Override
	public Result<List<String>> followers(String userId, String password) {
		Log.info(() -> format("followers : userId = %s, pwd = %s\n", userId, password));

		var query = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);		
		return errorOrValue( okUser(userId, password), DB.sql(query, String.class));
	}

	@Override
	public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
		Log.info(() -> format("like : shortId = %s, userId = %s, isLiked = %s, pwd = %s\n", shortId, userId, isLiked, password));

		
		return errorOrResult( getShort(shortId), shrt -> {
			shortsCache.invalidate( shortId );
			
			var l = new Likes(userId, shortId, shrt.getOwnerId());
			return errorOrVoid( okUser( userId, password), isLiked ? DB.insertOne( l ) : DB.deleteOne( l ));	
		});
	}

	@Override
	public Result<List<String>> likes(String shortId, String password) {
		Log.info(() -> format("likes : shortId = %s, pwd = %s\n", shortId, password));

		return errorOrResult( getShort(shortId), shrt -> {
			
			var query = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);					
			
			return errorOrValue( okUser( shrt.getOwnerId(), password ), DB.sql(query, String.class));
		});
	}

	@Override
	public Result<List<String>> getFeed(String userId, String password) {
		Log.info(() -> format("getFeed : userId = %s, pwd = %s\n", userId, password));

		final var QUERY_FMT = """
				SELECT s.shortId, s.timestamp FROM Short s WHERE	s.ownerId = '%s'				
				UNION			
				SELECT s.shortId, s.timestamp FROM Short s, Following f 
					WHERE 
						f.followee = s.ownerId AND f.follower = '%s' 
				ORDER BY s.timestamp DESC""";

		return errorOrValue( okUser( userId, password), DB.sql( format(QUERY_FMT, userId, userId), String.class));		
	}
		
	protected Result<User> okUser( String userId, String pwd) {
		try {
			return usersCache.get( new Credentials(userId, pwd));
		} catch (Exception x) {
			x.printStackTrace();
			return Result.error(INTERNAL_ERROR);
		}
	}
	
	private Result<Void> okUser( String userId ) {
		var res = okUser( userId, "");
		if( res.error() == FORBIDDEN )
			return ok();
		else
			return error( res.error() );
	}
	
	protected Result<Short> shortFromCache( String shortId ) {
		try {
			return shortsCache.get(shortId);
		} catch (ExecutionException e) {
			e.printStackTrace();
			return error(INTERNAL_ERROR);
		}
	}

	// Extended API 
	
	@Override
	public Result<Void> deleteAllShorts(String userId, String password, String token) {
		Log.info(() -> format("deleteAllShorts : userId = %s, password = %s, token = %s\n", userId, password, token));

		if( ! Token.matches( token ) )
			return error(FORBIDDEN);
		
		return DB.transaction( (hibernate) -> {
			
			usersCache.invalidate( new Credentials(userId, password) );
			
			//delete shorts
			var query1 = format("SELECT * FROM Short s WHERE s.ownerId = '%s'", userId);		
			hibernate.createNativeQuery(query1, Short.class).list().forEach( s -> {
				shortsCache.invalidate( s.getShortId() );
				hibernate.remove(s);
			});
			
			//delete follows
			var query2 = format("SELECT * FROM Following f WHERE f.follower = '%s' OR f.followee = '%s'", userId, userId);		
			hibernate.createNativeQuery(query2, Following.class).list().forEach( hibernate::remove );
			
			//delete likes
			var query3 = format("SELECT * FROM Likes l WHERE l.ownerId = '%s' OR l.userId = '%s'", userId, userId);		
			hibernate.createNativeQuery(query3, Likes.class).list().forEach( l -> {
				shortsCache.invalidate( l.getShortId() );
				hibernate.remove(l);
			});
		});
	}


	
	private String getLeastLoadedBlobServerURI() {
		try {
			var servers = blobCountCache.get(BLOB_COUNT);
			
			var	leastLoadedServer = servers.entrySet()
					.stream()
					.sorted( (e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
					.findFirst();
			
			if( leastLoadedServer.isPresent() )  {
				var uri = leastLoadedServer.get().getKey();
				servers.compute( uri, (k, v) -> v + 1L);				
				return uri;
			}
		} catch( Exception x ) {
			x.printStackTrace();
		}
		return "?";
	}
	
	static record BlobServerCount(String baseURI, Long count) {};
	
	private long totalShortsInDatabase() {
		var hits = DB.sql("SELECT count('*') FROM Short", Long.class);
		return 1L + (hits.isEmpty() ? 0L : hits.get(0));
	}

	
	
	
}

