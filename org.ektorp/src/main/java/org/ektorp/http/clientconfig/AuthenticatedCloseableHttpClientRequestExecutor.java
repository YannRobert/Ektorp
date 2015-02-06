package org.ektorp.http.clientconfig;

import org.apache.http.HttpHost;
import org.apache.http.client.AuthCache;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.ektorp.http.HttpResponse;

import javax.annotation.PostConstruct;
import java.io.IOException;

public class AuthenticatedCloseableHttpClientRequestExecutor extends CloseableHttpClientRequestExecutor {

	private HttpClientContext localContext;

	@PostConstruct
	@Override
	public void init() {
		super.init();
		localContext = initHttpClientContext();
	}

	public HttpClientContext initHttpClientContext() {
		HttpHost target = getHttpHost(locateHttpClient(false));

		// Create AuthCache instance
		AuthCache authCache = new BasicAuthCache();
		// Generate BASIC scheme object and add it to the local auth cache
		BasicScheme basicAuth = new BasicScheme();
		authCache.put(target, basicAuth);

		// Add AuthCache to the execution context
		HttpClientContext localContext = HttpClientContext.create();
		localContext.setAuthCache(authCache);
		return localContext;
	}

	@Override
	public HttpResponse executeRequest(HttpUriRequest request, boolean useBackend) throws IOException {
		if (localContext == null) {
			throw new IllegalStateException();
		}
		;
		HttpClient client = null;
		try {
			client = locateHttpClient(useBackend);

			final org.apache.http.HttpResponse response;
			if (useBackend) {
				response = client.execute(request, localContext);
			} else {
				response = client.execute(getHttpHost(client), request, localContext);
			}

			return createHttpResponse(response, request);
		} finally {
			releaseHttpClient(client);
		}
	}

}
