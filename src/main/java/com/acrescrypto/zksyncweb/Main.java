package com.acrescrypto.zksyncweb;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.ProcessingException;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.ServerConfiguration;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpContainer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.utility.Util;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;

public class Main {
	public final static String BASE_URI = "http://0.0.0.0:8080/";

	public static HttpServer startServer() throws IOException, URISyntaxException {
		ResourceConfig rc = new ResourceConfig();
		rc
			.register(ObjectMapperProvider.class)
			.register(JacksonFeature.class)
			.register(CustomLoggingFilter.class)
			.packages("com.acrescrypto.zksyncweb.exceptionmappers")
			.packages("com.acrescrypto.zksyncweb.resources");
		
		URI uri = new URI(BASE_URI);
		HttpServer server = GrizzlyHttpServerFactory.createHttpServer(uri);
		HttpHandler handler = ContainerFactory.createContainer(
				GrizzlyHttpContainer.class, rc);
		
		ServerConfiguration config = server.getServerConfiguration();
		config.addHttpHandler(handler, "/");

		server.start();
		return server;
	}

	public static void main(String[] args) {
		// assume SLF4J is bound to logback in the current environment
		LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
		// print logback's internal status
		StatusPrinter.print(lc);
		
		try {
			Util.launchTime();
			State.sharedState();
			startServer();
			while(true) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					break;
				}
			}
		} catch (ProcessingException | IOException | URISyntaxException e) {
			throw new Error("Unable to create HTTP server.", e);
		}
	}
}
