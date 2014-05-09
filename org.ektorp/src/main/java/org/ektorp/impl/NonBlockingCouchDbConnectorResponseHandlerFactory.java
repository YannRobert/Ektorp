package org.ektorp.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ektorp.CouchDbConnector;
import org.ektorp.http.ClassInstanceResponseHandler;
import org.ektorp.http.ResponseCallback;

/**
 * In a multi-threaded environment, when multiple threads are using Jackson as the very same time, threads can be blocked by synchronization happening into Jackson internal classes (regarding caching of class definitions).
 * In some case, when many threads are used to load from or save to the database, this can badly affect performances.
 * 
 * Using this class, a different ObjectMapper instance will be used by each Thread. This permits to avoid the synchronization problem in Jackson.
 * ObjectMappers instances are maintained in a ThreadLocal, so that is can be reused. (It is recommended to reuse threads, avoid creating new Threads)
 * 
 * For single-threaded application, this may also be interesting to use it, as the ResponseHandler instances are pooled, avoid to create a new temporary Object on each invocation.
 * 
 * For most applications, this should therefore be better than DefaultCouchDbConnectorResponseHandlerFactory.
 * 
 * usage : stdCouchDbConnector.setCouchDbConnectorResponseHandlerFactory(new NonBlockingCouchDbConnectorResponseHandlerFactory(stdCouchDbConnector));
 * 
 */
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
