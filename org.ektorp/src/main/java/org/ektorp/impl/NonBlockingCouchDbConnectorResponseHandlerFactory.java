package org.ektorp.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ektorp.CouchDbConnector;
import org.ektorp.http.ClassInstanceResponseHandler;
import org.ektorp.http.ResponseCallback;

public class NonBlockingCouchDbConnectorResponseHandlerFactory implements CouchDbConnectorResponseHandlerFactory {

	private final ThreadLocal<ClassInstanceResponseHandler> classInstanceResponseHandlerThreadLocal = new ThreadLocal<ClassInstanceResponseHandler>() {

		@Override
		protected ClassInstanceResponseHandler initialValue() {
			ObjectMapper objectMapper = getObjectMapperFactory().createObjectMapper(couchDbConnector);
			ClassInstanceResponseHandler result = new ClassInstanceResponseHandler(objectMapper);
			return result;
		}

	};

	private final ThreadLocal<EntityUpdateResponseHandler> entityUpdateResponseHandlerThreadLocal = new ThreadLocal<EntityUpdateResponseHandler>() {

		@Override
		protected EntityUpdateResponseHandler initialValue() {
			ObjectMapper objectMapper = getObjectMapperFactory().createObjectMapper(couchDbConnector);
			EntityUpdateResponseHandler result = new EntityUpdateResponseHandler(objectMapper);
			return result;
		}

	};

	private final StdCouchDbConnector couchDbConnector;

	public NonBlockingCouchDbConnectorResponseHandlerFactory(StdCouchDbConnector couchDbConnector) {
		super();
		this.couchDbConnector = couchDbConnector;
	}
	
	public ObjectMapperFactory getObjectMapperFactory() {
        return couchDbConnector.getObjectMapperFactory();
    }
	
	@Override
	public EntityUpdateResponseHandler getEntityUpdateResponseHandler(Object o, String id) {
		EntityUpdateResponseHandler responseHandler = entityUpdateResponseHandlerThreadLocal.get();
		responseHandler.setEntityInfo(o, id);
		return responseHandler;
	}

	@Override
	public <T> ResponseCallback<T> getClassInstanceResponseHandler(final Class<T> c) {
		ClassInstanceResponseHandler responseHandler = classInstanceResponseHandlerThreadLocal.get();
		responseHandler.setClazz(c);
		return responseHandler;
	}

}
