package org.ektorp.http.clientconfig;

import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.ektorp.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;

public class ThreadLocalCloseableHttpClientRequestExecutor extends HttpClientRequestExecutor {

	private final static Logger LOG = LoggerFactory.getLogger(ThreadLocalCloseableHttpClientRequestExecutor.class);

	private String serverHostName;

	private Integer serverHostPort;

	private boolean usingTLS;

	protected HttpClientBuilder clientBuilder;

	protected HttpClientBuilder backendBuilder;

	protected final ThreadLocal<CloseableHttpClient> clientThreadLocal = new ThreadLocal<CloseableHttpClient>();

	protected final ThreadLocal<CloseableHttpClient> backendThreadLocal = new ThreadLocal<CloseableHttpClient>();

	public ThreadLocalCloseableHttpClientRequestExecutor() {
		super();
	}

	@PostConstruct
	public void init() {
		Assert.notNull(serverHostName);
		Assert.notNull(serverHostPort);
	}

	/*
	@Override
	public HttpResponse executeRequest(HttpUriRequest request, boolean useBackend) throws IOException {
		HttpClient client = null;
		try {
			client = locateHttpClient(useBackend);
			final org.apache.http.HttpResponse response;
			response = client.execute(getHttpHost(client), request);
			return createHttpResponse(response, request);
		} finally {
			releaseHttpClient(client);
		}
	}
	*/

	@Override
	public HttpClient locateHttpClient(boolean useBackend) {
		if (useBackend) {
			return getOrCreateHttpClient(backendBuilder, backendThreadLocal);
		} else {
			return getOrCreateHttpClient(clientBuilder, clientThreadLocal);
		}
	}

	@Override
	public void releaseHttpClient(HttpClient client) throws IOException {
		// nothing to do
	}

	@Override
	public void shutdown() {
		// the ThreadLocal does not create a new value if it was not initialized already
		// so it is safe to use ThreadLocal.get() it won't create a new useless instance
		closeClient(clientThreadLocal.get());
		closeClient(backendThreadLocal.get());
	}

	@Override
	public HttpHost getHttpHost(HttpClient client) {
		return new HttpHost(serverHostName, serverHostPort, usingTLS ? "https" : "http");
	}

	protected void closeClient(CloseableHttpClient c) {
		if (c != null) {
			try {
				c.close();
			} catch (IOException e) {
				LOG.warn("IOException while closing CloseableHttpClient", e);
			}
		} else {
			LOG.debug("CloseableHttpClient was not set for current Thread");
		}
	}

	protected static CloseableHttpClient getOrCreateHttpClient(HttpClientBuilder builder, ThreadLocal<CloseableHttpClient> threadLocal) {
		CloseableHttpClient result = threadLocal.get();
		if (result == null) {
			result = builder.build();
			threadLocal.set(result);
		}
		return result;
	}

	public void setClientBuilder(HttpClientBuilder clientBuilder) {
		this.clientBuilder = clientBuilder;
	}

	public void setBackendBuilder(HttpClientBuilder backendBuilder) {
		this.backendBuilder = backendBuilder;
	}

	public void setServerHostName(String serverHostName) {
		this.serverHostName = serverHostName;
	}

	public void setServerHostPort(Integer serverHostPort) {
		this.serverHostPort = serverHostPort;
	}

	public void setUsingTLS(boolean usingTLS) {
		this.usingTLS = usingTLS;
	}
}
