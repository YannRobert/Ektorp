package org.ektorp.http.clientconfig;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.protocol.HttpContext;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class CouchDbDefaultRoutePlannerTest {

	@Test
	public void shouldSupportNullTargetHost() throws HttpException {
		final HttpRequest request = mock(HttpRequest.class);
		final HttpContext context = mock(HttpContext.class);

		final CouchDbDefaultRoutePlanner tested = new CouchDbDefaultRoutePlanner();

		final HttpRoute result = tested.determineRoute(null, request, context);
		assertNotNull(result);
	}

}
