package tukano.impl.grpc.clients;

import static tukano.impl.grpc.common.DataModelAdaptor.GrpcUser_to_User;
import static tukano.impl.grpc.common.DataModelAdaptor.User_to_GrpcUser;

import java.util.ArrayList;
import java.util.List;

import tukano.api.User;
import tukano.api.java.Result;
import tukano.api.java.Users;
import tukano.impl.grpc.generated_java.UsersGrpc;
import tukano.impl.grpc.generated_java.UsersProtoBuf.CreateUserArgs;
import tukano.impl.grpc.generated_java.UsersProtoBuf.DeleteUserArgs;
import tukano.impl.grpc.generated_java.UsersProtoBuf.GetUserArgs;
import tukano.impl.grpc.generated_java.UsersProtoBuf.SearchUserArgs;
import tukano.impl.grpc.generated_java.UsersProtoBuf.UpdateUserArgs;

public class GrpcUsersClient extends GrpcClient implements Users {

	final UsersGrpc.UsersBlockingStub stub;

	public GrpcUsersClient(String serverURI) {
		super(serverURI);
		this.stub = UsersGrpc.newBlockingStub( super.channel );	
	}

	public Result<String> createUser(User user) {
		return super.toJavaResult(() -> {
			
			var res = stub.createUser(CreateUserArgs.newBuilder()
				.setUser(User_to_GrpcUser(user))
				.build());
			
			return res.getUserId();
		});
	}

	public Result<User> getUser(String userId, String pwd) {
		return super.toJavaResult(()-> {
			var res = stub.getUser(GetUserArgs.newBuilder()
				.setUserId(userId)
				.setPassword(pwd)
				.build());
			return GrpcUser_to_User(res.getUser());
		});
	}

	public Result<User> updateUser(String userId, String pwd, User user) {
		return super.toJavaResult(() -> {	
			var res = stub.updateUser(UpdateUserArgs.newBuilder()
				.setUserId(userId)
				.setPassword(pwd)
				.setUser(User_to_GrpcUser(user))
				.build());
		
			return GrpcUser_to_User(res.getUser());
		});
	}

	public Result<User> deleteUser(String userId, String pwd) {
		return super.toJavaResult(() -> {
			var res = stub.deleteUser(DeleteUserArgs.newBuilder()
					.setUserId(userId)
					.setPassword(pwd)
					.build());
			return GrpcUser_to_User(res.getUser());
		});
	}

	public Result<List<User>> searchUsers(String pattern) {
		return super.toJavaResult(() -> {
			var res = stub.searchUsers(SearchUserArgs.newBuilder()
				.setPattern(pattern)
				.build());

			var list = new ArrayList<User>();
			res.forEachRemaining( user -> list.add( GrpcUser_to_User(user)) );
			return list;
		});
	}
}
