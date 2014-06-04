package org.ektorp.http.clientconfig;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.mockito.Mockito.*;

public class ThreadLocalCloseableHttpClientRequestExecutorTest {

	private final static Logger LOG = LoggerFactory.getLogger(ThreadLocalCloseableHttpClientRequestExecutorTest.class);

	@Before
	public void before() {
		LOG.info("before()");
	}

	@Test
	public void shouldReuseSameInstanceOnMultipleInvocationBySameThreadOnBackend() throws IOException, URISyntaxException {
		HttpClientBuilder backendBuilder = mock(HttpClientBuilder.class);

		HttpUriRequest request = mock(HttpUriRequest.class);
		doReturn(new URI("whatever")).when(request).getURI();

		CloseableHttpClient closeableHttpClient = mock(CloseableHttpClient.class, new ThrowsException(new UnsupportedOperationException("Unsupported mock usage")));
		doNothing().when(closeableHttpClient).close();

		doReturn(closeableHttpClient).doThrow(new UnsupportedOperationException("build method should only be called once")).when(backendBuilder).build();

		Header etagHeader = mock(Header.class);
		doReturn("1234567").when(etagHeader).getValue();

		CloseableHttpResponse response = mock(CloseableHttpResponse.class);
		doReturn(etagHeader).when(response).getFirstHeader("ETag");

		doReturn(response).when(closeableHttpClient).execute(eq(request));

		ThreadLocalCloseableHttpClientRequestExecutor tested = new ThreadLocalCloseableHttpClientRequestExecutor();
		tested.setBackendBuilder(backendBuilder);
		tested.setServerHostName(RandomStringUtils.random(8));
		tested.setServerHostPort(RandomUtils.nextInt(Short.MAX_VALUE));
		tested.init();

		tested.executeRequest(request, true);

		verify(closeableHttpClient, times(1)).execute(eq(request));
		verify(closeableHttpClient, times(0)).close();

		tested.executeRequest(request, true);

		verify(closeableHttpClient, times(2)).execute(eq(request));
		verify(closeableHttpClient, times(0)).close();
		
		tested.shutdown();
		verify(closeableHttpClient, times(1)).close();
	}

	@Test
	public void shouldReuseSameInstanceOnMultipleInvocationBySameThreadOnClient() throws IOException, URISyntaxException {
		HttpClientBuilder clientBuilder = mock(HttpClientBuilder.class);

		HttpUriRequest request = mock(HttpUriRequest.class);
		doReturn(new URI("whatever")).when(request).getURI();

		CloseableHttpClient closeableHttpClient = mock(CloseableHttpClient.class, new ThrowsException(new UnsupportedOperationException("Unsupported mock usage")));
		doNothing().when(closeableHttpClient).close();

		doReturn(closeableHttpClient).doThrow(new UnsupportedOperationException("build method should only be called once")).when(clientBuilder).build();

		Header etagHeader = mock(Header.class);
		doReturn("1234567").when(etagHeader).getValue();

		CloseableHttpResponse response = mock(CloseableHttpResponse.class);
		doReturn(etagHeader).when(response).getFirstHeader("ETag");

		doReturn(response).when(closeableHttpClient).execute(any(HttpHost.class), eq(request));

		ThreadLocalCloseableHttpClientRequestExecutor tested = new ThreadLocalCloseableHttpClientRequestExecutor();
		tested.setClientBuilder(clientBuilder);
		tested.setServerHostName(RandomStringUtils.random(8));
		tested.setServerHostPort(RandomUtils.nextInt(Short.MAX_VALUE));
		tested.init();

		tested.executeRequest(request, false);

		verify(closeableHttpClient, times(1)).execute(any(HttpHost.class), eq(request));
		verify(closeableHttpClient, times(0)).close();

		tested.executeRequest(request, false);

		verify(closeableHttpClient, times(2)).execute(any(HttpHost.class), eq(request));
		verify(closeableHttpClient, times(0)).close();

		tested.shutdown();
		verify(closeableHttpClient, times(1)).close();
	}


}
