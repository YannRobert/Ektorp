package org.ektorp.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ektorp.http.ClassInstanceResponseHandler;
import org.ektorp.http.ResponseCallback;

public class DefaultCouchDbConnectorResponseHandlerFactory implements CouchDbConnectorResponseHandlerFactory {

	private ObjectMapper objectMapper;

	public DefaultCouchDbConnectorResponseHandlerFactory() {
		super();
	}
	
	public DefaultCouchDbConnectorResponseHandlerFactory(ObjectMapper objectMapper) {
		super();
		this.setObjectMapper(objectMapper);
	}

	@Override
	public EntityUpdateResponseHandler getEntityUpdateResponseHandler(Object o, String id) {
		return new EntityUpdateResponseHandler(getObjectMapper(), o, id);
	}

	@Override
	public <T> ResponseCallback<T> getClassInstanceResponseHandler(final Class<T> c) {
		return new ClassInstanceResponseHandler(getObjectMapper(), c);
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

}
