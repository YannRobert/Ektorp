package org.ektorp.http.clientconfig;

import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.protocol.HttpContext;
import org.ektorp.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchDbDefaultRoutePlanner extends DefaultRoutePlanner {

	private final static Logger LOG = LoggerFactory.getLogger(CouchDbDefaultRoutePlanner.class);

	private String serverHostName = "localhost";

	private Integer serverHostPort = 5984;

	private String username;

	private String password;

	private boolean usingTLS = false;

	public CouchDbDefaultRoutePlanner() {
		this(DefaultSchemePortResolver.INSTANCE);
	}

	public CouchDbDefaultRoutePlanner(org.apache.http.conn.SchemePortResolver schemePortResolver) {
		super(schemePortResolver);
	}

	@Override
	public HttpRoute determineRoute(final HttpHost host, final HttpRequest request, final HttpContext context) throws HttpException {
		final HttpHost target;
		if (host == null) {
			String serverHostNameLocal = getServerHostName();
			Integer serverHostPortLocal = getServerHostPort();
			Assert.notNull(serverHostNameLocal);
			Assert.notNull(serverHostPortLocal);
			//
			target = new HttpHost(serverHostNameLocal, serverHostPortLocal, usingTLS ? "https" : "http");
		} else {
			target = host;
		}
		LOG.trace("target = {}", target);

		if (username != null && password != null) {
			UsernamePasswordCredentials usernamePasswordCredentials = new UsernamePasswordCredentials(username, password);
			LOG.trace("usernamePasswordCredentials = {}", usernamePasswordCredentials);
			request.addHeader(new BasicScheme().authenticate(usernamePasswordCredentials, request));
		}
		

		return super.determineRoute(target, request, context);
	}

	public String getServerHostName() {
		return serverHostName;
	}

	public Integer getServerHostPort() {
		return serverHostPort;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public boolean isUsingTLS() {
		return usingTLS;
	}

	public void setServerHostName(String serverHostName) {
		this.serverHostName = serverHostName;
	}

	public void setServerHostPort(Integer serverHostPort) {
		this.serverHostPort = serverHostPort;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setUsingTLS(boolean usingTLS) {
		this.usingTLS = usingTLS;
	}
}
