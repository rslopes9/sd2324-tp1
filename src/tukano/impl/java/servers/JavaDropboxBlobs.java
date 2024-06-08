package tukano.impl.java.servers;

import static java.lang.String.format;
import static tukano.api.java.Result.error;
import static tukano.api.java.Result.ok;
import static tukano.api.java.Result.ErrorCode.FORBIDDEN;
import static tukano.api.java.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.java.Result.ErrorCode.CONFLICT;
import static tukano.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.java.Result.ErrorCode.NOT_FOUND;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import org.pac4j.scribe.builder.api.DropboxApi20;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;
import com.google.rpc.context.AttributeContext.Response;

import tukano.api.java.Result;
import tukano.impl.api.java.ExtendedBlobs;
import tukano.impl.java.clients.Clients;
import utils.Hash;
import utils.Hex;

public class JavaDropboxBlobs implements ExtendedBlobs{
	private static Logger Log = Logger.getLogger(JavaDropboxBlobs.class.getName());

	//private static final int CHUNK_SIZE = 4096;
	
	private static final String CONTENT_TYPE_HDR = "Content-Type";
	private static final String JSON_CONTENT_TYPE = "application/octet-stream";
	 private static final String DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload";
    static String apiKey;
    static String apiSecret;
    static String accessTokenStr;
    private OAuth2AccessToken accessToken;
    private OAuth20Service service;
    Gson json = new Gson();

	public JavaDropboxBlobs(String apiKey, String apiSecret, String accessTokenStr, boolean ignoreState) {		
		try {
			service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
			accessToken = new OAuth2AccessToken(accessTokenStr);
		} catch (Exception x) {
			x.printStackTrace();
			System.exit(0);
		}
		
		if(ignoreState) {
			ignoreState();
		}
	}
	
	public record CreateV2Args(String path, boolean autorename) {
		
	}

	@Override
	public Result<Void> upload(String blobId, byte[] bytes) {
		Log.info(() -> format("upload : blobId = %s, sha256 = %s\n", blobId, Hex.of(Hash.sha256(bytes))));

		if (!validBlobId(blobId)) {

			return error(FORBIDDEN);
		}
			
		
		
		String[] parts = blobId.split("-");
        if (parts.length != 2) {

            return Result.error(FORBIDDEN);  
        }
        String userId = parts[0];
        String singularBlobId = parts[1];
        
        String directoryPath = "/" + userId + "/" + singularBlobId;
		try {
			OAuthRequest request = new OAuthRequest(Verb.POST, DROPBOX_UPLOAD_URL);
			request.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
			
			String json_args = json.toJson(new CreateV2Args(directoryPath, false));
			
			request.setPayload( json_args );
			
			request.addHeader("Dropbox-API-Arg", json_args);
			
			service.signRequest(accessToken, request);

			var response = service.execute(request);
			
			if (response.getCode() != 200) {
			    throw new RuntimeException(String.format("Failed to create directory: %s, Status: %d, \nReason: %s\n",
			    		directoryPath, response.getCode(), response.getBody()));
			}

				
			
		} catch (Exception x) {
			x.printStackTrace();
		}
		
		return ok();



	}

	@Override
	public Result<byte[]> download(String blobId) {
	    Log.info(() -> format("download : blobId = %s\n", blobId));
	    if (!validBlobId(blobId)) {
	        return error(FORBIDDEN);
	    }

	    String[] parts = blobId.split("-");
	    if (parts.length != 2) {
	        return error(BAD_REQUEST);  
	    }
	    
	    String userId = parts[0];
	    String singularBlobId = parts[1];
	    
	    String filePath = "/" + userId + "/" + singularBlobId;
System.out.println("pelagiod");
	    try {
	        OAuthRequest request = new OAuthRequest(Verb.POST, "https://content.dropboxapi.com/2/files/download");
	        request.addHeader(CONTENT_TYPE_HDR, "application/octet-stream"); // Set appropriate Content-Type

	        Map<String, String> args = new HashMap<>();
	        args.put("path", filePath);
	        String json_args = json.toJson(args);
	        request.addHeader("Dropbox-API-Arg", json_args);

	        service.signRequest(accessToken, request);
	        var response = service.execute(request);

	        if (response.getCode() != 200) {
	        	throw new RuntimeException(String.format("Failed to download file: %s, Status: %d, \nReason: %s\n",
		                   filePath, response.getCode(), response.getBody()));
	        } else {
	        	System.out.println(response.getStream().readAllBytes());
	            return ok(response.getStream().readAllBytes());
	        }
	    } catch (Exception x) {
	        x.printStackTrace();
	        return error(INTERNAL_ERROR);
	    }
	}

	@Override
	public Result<Void> delete(String blobId, String token) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result<Void> deleteAllBlobs(String userId, String token) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private boolean validBlobId(String blobId) {
		return Clients.ShortsClients.get().getShort(blobId).isOK();
	}
	
	public Result<Void> ignoreState() {
	    try {
	        deleteAllFilesAndFolders("");
	        return ok();
	    } catch (Exception e) {
	        e.printStackTrace();
	        return error(INTERNAL_ERROR);
	    }
	}
	
	private void deleteAllFilesAndFolders(String path) throws IOException, InterruptedException, ExecutionException {
	    OAuthRequest listRequest = new OAuthRequest(Verb.POST, "https://api.dropboxapi.com/2/files/list_folder");
	    listRequest.addHeader(CONTENT_TYPE_HDR, "application/json");
	    String listArgs = json.toJson(Map.of("path", path, "recursive", true));
	    listRequest.setPayload(listArgs);
	    service.signRequest(accessToken, listRequest);

	    var listResponse = service.execute(listRequest);
	    if (listResponse.getCode() != 200) {
	        throw new RuntimeException(String.format("Failed to list folder: %s, Status: %d, \nReason: %s\n",
	                path, listResponse.getCode(), listResponse.getBody()));
	    }

	    Map<String, Object> listResult = json.fromJson(listResponse.getBody(), Map.class);
	    List<Map<String, Object>> entries = (List<Map<String, Object>>) listResult.get("entries");

	    for (Map<String, Object> entry : entries) {
	        String entryPath = (String) entry.get("path_lower");
	        deletePath(entryPath);
	    }
	}

	private void deletePath(String path) throws IOException, InterruptedException, ExecutionException {
	    OAuthRequest deleteRequest = new OAuthRequest(Verb.POST, "https://api.dropboxapi.com/2/files/delete_v2");
	    deleteRequest.addHeader(CONTENT_TYPE_HDR, "application/json");
	    String deleteArgs = json.toJson(Map.of("path", path));
	    deleteRequest.setPayload(deleteArgs);
	    service.signRequest(accessToken, deleteRequest);

	    var deleteResponse = service.execute(deleteRequest);
	    if (deleteResponse.getCode() != 200) {
	        throw new RuntimeException(String.format("Failed to delete path: %s, Status: %d, \nReason: %s\n",
	                path, deleteResponse.getCode(), deleteResponse.getBody()));
	    }
	}

}
