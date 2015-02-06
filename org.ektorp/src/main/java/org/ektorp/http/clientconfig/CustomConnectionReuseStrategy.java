package org.ektorp.http.clientconfig;

import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConnectionReuseStrategy to be used when preemptive authentication is not working correctly, and the CouchDB authentication is refused.<br>
 * In that case, the connection cannot be reused, at least with Cloudant Hosted Service.
 */
public class CustomConnectionReuseStrategy implements ConnectionReuseStrategy {

    private final static Logger log = LoggerFactory.getLogger(CustomConnectionReuseStrategy.class);

    private final String logMessagePattern = "response = {}, requestFullyTransmitted = {}, method = {}, uri = {}, statusCode = {}, delegateResult = {}, result = {}";

    private final DefaultConnectionReuseStrategy delegate = DefaultConnectionReuseStrategy.INSTANCE;

    public CustomConnectionReuseStrategy() {
        super();
    }

    @Override
    public boolean keepAlive(HttpResponse response, HttpContext context) {
        HttpRequest request = (HttpRequest) context.getAttribute(HttpCoreContext.HTTP_REQUEST);
        Boolean requestFullyTransmitted = (Boolean) context.getAttribute(HttpCoreContext.HTTP_REQ_SENT);

        RequestLine requestLine = request.getRequestLine();
        String method = requestLine.getMethod();
        String uri = request.getRequestLine().getUri();

        final int statusCode = response.getStatusLine().getStatusCode();

        boolean delegateResult = delegate.keepAlive(response, context);
        boolean result = delegateResult;

        // in case the previous connection was not authenticated, we may not reuse the connection
        if (statusCode == 401) {
            result = false;
        }
        // in case of a Bad Gateway error, we should not reuse the connection
        if (statusCode == 502) {
            result = false;
        }

        Object[] logParameters = {response, requestFullyTransmitted, method, uri, statusCode, delegateResult, result};

        // if the connection cannot be reused, then log using INFO level, else use DEBUG level
        if (result) {
            log.debug(logMessagePattern, logParameters);
        } else {
            log.info(logMessagePattern, logParameters);
        }

        return result;
    }
}
