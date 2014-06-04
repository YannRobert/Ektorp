package org.ektorp.http.clientconfig;

import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.ektorp.http.HttpResponse;
import org.ektorp.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;

public class CloseableHttpClientRequestExecutor extends HttpClientRequestExecutor {

	private final static Logger LOG = LoggerFactory.getLogger(CloseableHttpClientRequestExecutor.class);

	private String serverHostName;

	private Integer serverHostPort;

	private HttpClientBuilder clientBuilder;

	private HttpClientBuilder backendBuilder;

	private CloseableHttpClient client;

	private CloseableHttpClient backend;

	public CloseableHttpClientRequestExecutor() {
		super();
	}

	@PostConstruct
	public void init() {
		Assert.notNull(serverHostName);
		Assert.notNull(serverHostPort);
		if (clientBuilder != null) {
			client = clientBuilder.build();
		}
		if (backendBuilder != null) {
			backend = backendBuilder.build();
		}
	}

	@Override
	public HttpResponse executeRequest(HttpUriRequest request, boolean useBackend) throws IOException {
		return super.executeRequest(request, false);
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
			return backend;
		} else {
			return client;
		}
	}

	@Override
	public void releaseHttpClient(HttpClient client) throws IOException {
		// nothing to do
	}

	@Override
	public void shutdown() {
		closeClient(client);
		closeClient(backend);
	}

	@Override
	public HttpHost getHttpHost(HttpClient client) {
		return new HttpHost(serverHostName, serverHostPort);
	}

	protected void closeClient(CloseableHttpClient c) {
		try {
			c.close();
		} catch (IOException e) {
			LOG.error("IOException while closing CloseableHttpClient");
		}
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
}
