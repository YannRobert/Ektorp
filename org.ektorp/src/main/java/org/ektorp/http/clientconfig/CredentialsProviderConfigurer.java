package org.ektorp.http.clientconfig;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.ektorp.http.PreemptiveAuthRequestInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CredentialsProviderConfigurer {

	public static class DefaultCredentialsProvider implements CredentialsProvider {

		private final static Logger LOG = LoggerFactory.getLogger(DefaultCredentialsProvider.class);

		private final Map<AuthScope, Credentials> credentialsMap = new ConcurrentHashMap<AuthScope, Credentials>();

		public DefaultCredentialsProvider() {
			super();
		}

		@Override
		public void setCredentials(AuthScope authScope, Credentials credentials) {
			credentialsMap.put(authScope, credentials);
		}

		@Override
		public Credentials getCredentials(AuthScope authScope) {
			Credentials result = credentialsMap.get(authScope);
			if (result == null) {
				result = credentialsMap.get(new AuthScope(authScope.getHost(), authScope.getPort(), AuthScope.ANY_REALM));
			}
			if (LOG.isTraceEnabled()) {
				LOG.trace("getCredentials({}) : result = {}", new Object[]{authScope, result});
			}
			return result;
		}

		@Override
		public void clear() {
			credentialsMap.clear();
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append(this.getClass().getName()).append("{");
			builder.append("credentialsMap=").append(credentialsMap);
			builder.append("}");
			return builder.toString();
		}

	}

	private final static Logger LOG = LoggerFactory.getLogger(CredentialsProviderConfigurer.class);

	private String username;

	private String password;

	private String host;

	private int port;

	private CredentialsProvider credentialsProvider;

	@PostConstruct
	public void configure() {
		configure(this.credentialsProvider);
	}

	public void configure(CredentialsProvider credentialsProvider) {
		if (username == null || password == null) {
			return;
		}
		AuthScope authScope = new AuthScope(host, port, AuthScope.ANY_REALM);
		UsernamePasswordCredentials usernamePasswordCredentials = new UsernamePasswordCredentials(username, password);
		LOG.info("authScope = {}, usernamePasswordCredentials = {}", new Object[]{authScope, usernamePasswordCredentials});
		credentialsProvider.setCredentials(authScope, usernamePasswordCredentials);
		LOG.info("credentialsProvider = {}", credentialsProvider);
	}

	public void configure(DefaultHttpClient client) {
		configure(client.getCredentialsProvider());
		client.addRequestInterceptor(new PreemptiveAuthRequestInterceptor(), 0);
	}

	public void configure(HttpClientBuilder clientBuilder) {
		clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		clientBuilder.addInterceptorFirst(new PreemptiveAuthRequestInterceptor());
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setCredentialsProvider(CredentialsProvider credentialsProvider) {
		this.credentialsProvider = credentialsProvider;
	}
}
