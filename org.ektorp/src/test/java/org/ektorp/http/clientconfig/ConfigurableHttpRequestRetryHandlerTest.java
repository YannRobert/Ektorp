package org.ektorp.http.clientconfig;

import org.apache.http.HttpRequest;
import org.apache.http.RequestLine;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ConfigurableHttpRequestRetryHandlerTest {

	static {
		configureLogging();
	}
	
	@Test
	public void testDoNotIncludeStackTrace() {
		ConfigurableHttpRequestRetryHandler tested = new ConfigurableHttpRequestRetryHandler();
		tested.setIncludeExceptionInLogs(false);

		RequestLine requestLine = mock(RequestLine.class);

		HttpRequest httpRequest = mock(HttpRequest.class);
		doReturn(requestLine).when(httpRequest).getRequestLine();

		HttpContext httpContext = mock(HttpContext.class);
		doReturn(httpRequest).when(httpContext).getAttribute(ExecutionContext.HTTP_REQUEST);

		tested.retryRequest(new IOException("Fake Exception, do not print my stack trace"), 1, httpContext);
	}

	@Test
	public void testIncludeStackTrace() {
		ConfigurableHttpRequestRetryHandler tested = new ConfigurableHttpRequestRetryHandler();
		tested.setIncludeExceptionInLogs(true);
		
		RequestLine requestLine = mock(RequestLine.class);

		HttpRequest httpRequest = mock(HttpRequest.class);
		doReturn(requestLine).when(httpRequest).getRequestLine();

		HttpContext httpContext = mock(HttpContext.class);
		doReturn(httpRequest).when(httpContext).getAttribute(ExecutionContext.HTTP_REQUEST);

		tested.retryRequest(new IOException("Fake Exception, print my stack trace"), 1, httpContext);
	}

	private static void configureLogging() {
		ConsoleAppender console = new ConsoleAppender(); //create appender
		//configure the appender
		String PATTERN = "%d [%p|%c|%C{1}] %m%n";
		console.setLayout(new PatternLayout(PATTERN));
		//console.setThreshold(Level.TRACE);
		console.activateOptions();
		
		//add appender to any Logger (here is root)
		Logger.getRootLogger().addAppender(console);
		Logger.getLogger(ConfigurableHttpRequestRetryHandler.class).setLevel(Level.TRACE);
		Logger.getRootLogger().info("Started");
	}
	
}
