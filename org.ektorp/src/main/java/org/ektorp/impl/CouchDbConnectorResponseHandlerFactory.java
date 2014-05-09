package org.ektorp.impl;

import org.ektorp.http.ResponseCallback;

public interface CouchDbConnectorResponseHandlerFactory {

	EntityUpdateResponseHandler getEntityUpdateResponseHandler(Object o, String id);

	<T> ResponseCallback<T> getClassInstanceResponseHandler(Class<T> c);

}
