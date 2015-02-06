package org.ektorp.spring;

import org.apache.http.client.BackoffManager;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.AIMDBackoffManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.ektorp.http.StdHttpClient;
import org.ektorp.http.clientconfig.ConfigurableConnectionKeepAliveStrategy;
import org.ektorp.http.clientconfig.ConfigurableHttpRequestRetryHandler;
import org.ektorp.http.clientconfig.CustomConnectionReuseStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.DirectFieldAccessor;

import java.util.Properties;

/**
 * We need to override the afterPropertiesSet method here<br>
 * in order to introduce support to HTTP proxies.<br>
 */
public class ExtendedHttpClientFactoryBean extends HttpClientFactoryBean {

    /**
     * here we use slf4j because the original class we extends already use slf4j
     * and we mostly copied-out a lot of code from that class... and we don't want to modify it more than we need.
     */
    private static final Logger log = LoggerFactory.getLogger(HttpClientFactoryBean.class);

    private Properties couchDBProperties;

    private SSLSocketFactory sslSocketFactory;

    private String proxy;

    private int proxyPort;

    /**
     * This override the parent method and keep track of the parameter in a field for internal usage.
     */
    @Override
    public void setProperties(Properties p) {
        super.setProperties(p);
        this.couchDBProperties = p;
    }

    /**
     * This override the parent method and keep track of the parameter in a field for internal usage.
     */
    @Override
    public void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
        super.setSslSocketFactory(sslSocketFactory);
        this.sslSocketFactory = sslSocketFactory;
    }

    /**
     * Create the couchDB connection when starting the bean factory
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        if (couchDBProperties != null) {
            new DirectFieldAccessor(this).setPropertyValues(couchDBProperties);
        }
        log.info("Starting couchDb connector on {}:{}...", new Object[]{host, port});
        log.debug("host: {}", host);
        log.debug("port: {}", port);
        log.debug("url: {}", url);
        log.debug("maxConnections: {}", maxConnections);
        log.debug("connectionTimeout: {}", connectionTimeout);
        log.debug("socketTimeout: {}", socketTimeout);
        log.debug("autoUpdateViewOnChange: {}", autoUpdateViewOnChange);
        log.debug("testConnectionAtStartup: {}", testConnectionAtStartup);
        log.debug("cleanupIdleConnections: {}", cleanupIdleConnections);
        log.debug("enableSSL: {}", enableSSL);
        log.debug("relaxedSSLSettings: {}", relaxedSSLSettings);

        StdHttpClient.Builder builder = new StdHttpClient.Builder() {

            @Override
            public HttpClient configureClient() {
                final DefaultHttpClient result = (DefaultHttpClient) super.configureClient();
                result.setKeepAliveStrategy(new ConfigurableConnectionKeepAliveStrategy());
                result.setHttpRequestRetryHandler(new ConfigurableHttpRequestRetryHandler());
                result.setReuseStrategy(new CustomConnectionReuseStrategy());
                result.setBackoffManager(new BackoffManager() {

                    private final BackoffManager delegate = new AIMDBackoffManager((org.apache.http.impl.conn.PoolingClientConnectionManager) result.getConnectionManager());

                    @Override
                    public void backOff(HttpRoute route) {
                        log.info("backOff event : route = " + route);
                        delegate.backOff(route);
                    }

                    @Override
                    public void probe(HttpRoute route) {
                        log.info("probe event : route = " + route);
                        delegate.probe(route);
                    }

                });

                return result;
            }

            @Override
            protected HttpParams configureHttpParams() {
                HttpParams result = super.configureHttpParams();
                // Disabling stale connection check may result in slight performance improvement
                // at the risk of getting an I/O error
                // when executing a request over a connection that has been closed at the server side.
                HttpConnectionParams.setStaleCheckingEnabled(result, false);
                HttpConnectionParams.setSoKeepalive(result, true);
                result.setParameter("http.protocol.version", new org.apache.http.ProtocolVersion("HTTP", 1, 1));
                return result;
            }

            // the following method override may be a good solution or not
            // I need to perform more tests with it
            /*
			@Override
			public ClientConnectionManager configureConnectionManager(HttpParams params) {
				if (conman == null) {
					SchemeRegistry schemeRegistry = new SchemeRegistry();
					schemeRegistry.register(configureScheme());

					ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
					cm.setMaxTotal(maxConnections);
					cm.setDefaultMaxPerRoute(maxConnections);
					conman = cm;
				}

				if (cleanupIdleConnections) {
					IdleConnectionMonitor.monitor(conman);
				}
				return conman;
			}
			*/

        }
                .host(host)
                .port(port)
                .maxConnections(maxConnections)
                .connectionTimeout(connectionTimeout)
                .socketTimeout(socketTimeout)
                .username(username)
                .password(password)
                .cleanupIdleConnections(cleanupIdleConnections)
                .enableSSL(enableSSL)
                .relaxedSSLSettings(relaxedSSLSettings)
                .sslSocketFactory(sslSocketFactory)
                .caching(caching)
                .maxCacheEntries(maxCacheEntries)
                .maxObjectSizeBytes(maxObjectSizeBytes)
                .url(url)
                .proxy(proxy)
                .proxyPort(proxyPort);

        client = builder.build();

        if (testConnectionAtStartup) {
            testConnect(client);
        }

        configureAutoUpdateViewOnChange();
    }

    public void setProxy(String proxy) {
        this.proxy = proxy;
    }

    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }
}
