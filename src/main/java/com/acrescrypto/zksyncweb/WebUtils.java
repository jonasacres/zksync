package com.acrescrypto.zksyncweb;

import java.io.IOException;

public class WebUtils {
	public interface EmptyFieldActor { void act(); }
	public interface FieldActor<T> { void act(T b); }
	public interface FieldActorIOException<T> { void act(T b) throws IOException; }
	
	public static <T> void mapField(T field, FieldActor<T> actor) {
		if(field != null) {
			actor.act(field);
		}
	}
	
	public static <T> void mapFieldWithException(T field, FieldActorIOException<T> actor) throws IOException {
		if(field != null) {
			actor.act(field);
		}
	}
	
	public static void mapField(Boolean field, EmptyFieldActor trueActor, EmptyFieldActor falseActor) {
		if(field != null) {
			if(field) {
				trueActor.act();
			} else {
				falseActor.act();
			}
		}
	}

	public static double smooshInfinite(Double v) {
		if(Double.isInfinite(v) || Double.isNaN(v)) return -1.0;
		return v;
	}
}
