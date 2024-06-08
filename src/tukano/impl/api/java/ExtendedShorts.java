package tukano.impl.api.java;

import tukano.api.java.Result;
import tukano.api.java.Shorts;

public interface ExtendedShorts extends Shorts {

	Result<Void> deleteAllShorts( String userId, String password, String token );
	
}
