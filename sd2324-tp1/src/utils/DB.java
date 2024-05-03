package utils;

import java.util.List;
import java.util.function.Consumer;

import org.hibernate.Session;

import tukano.api.java.Result;

public class DB {

	public static <T> List<T> sql(String query, Class<T> clazz) {
		return Hibernate.getInstance().sql(query, clazz);
	}
		
	public static <T> Result<T> getOne(String id, Class<T> clazz) {
		return Hibernate.getInstance().getOne(id, clazz);
	}
	
	public static <T> Result<T> deleteOne(T obj) {
		return Hibernate.getInstance().deleteOne(obj);
	}
	
	public static <T> Result<T> updateOne(T obj) {
		return Hibernate.getInstance().updateOne(obj);
	}
	
	public static <T> Result<T> insertOne( T obj) {
		return Result.errorOrValue(Hibernate.getInstance().persistOne(obj), obj);
	}
	
	public static <T> Result<T> transaction( Consumer<Session> c) {
		return Hibernate.getInstance().execute( c::accept );
	}
}
