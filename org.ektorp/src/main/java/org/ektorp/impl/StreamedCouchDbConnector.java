package org.ektorp.impl;

import org.apache.http.HttpEntity;
import org.ektorp.CouchDbInstance;
import org.ektorp.DocumentOperationResult;
import org.ektorp.PurgeResult;
import org.ektorp.http.JacksonableEntity;
import org.ektorp.http.ResponseCallback;
import org.ektorp.util.Assert;
import org.ektorp.util.Documents;

import java.util.List;
import java.util.Map;

public class StreamedCouchDbConnector extends StdCouchDbConnector {

    public StreamedCouchDbConnector(String databaseName, CouchDbInstance dbInstance) {
        super(databaseName, dbInstance);
    }

    public StreamedCouchDbConnector(String databaseName, CouchDbInstance dbi, ObjectMapperFactory om) {
        super(databaseName, dbi, om);
    }

    {
        setCollectionBulkExecutor(new EntityCollectionBulkExecutor(dbURI, restTemplate, objectMapper));
        setInputStreamBulkExecutor(new InputStreamBulkEntityBulkExecutor(dbURI, restTemplate, objectMapper));
        setCouchDbConnectorResponseHandlerFactory(new NonBlockingCouchDbConnectorResponseHandlerFactory(this));
    }

    protected HttpEntity createHttpEntity(Object o) {
        return new JacksonableEntity(o, objectMapper);
    }

    @Override
    public void create(final Object o) {
        Assert.notNull(o, "Document may not be null");
        Assert.isTrue(Documents.isNew(o), "Object must be new");

        HttpEntity entity = createHttpEntity(o);

        String id = Documents.getId(o);
        DocumentOperationResult result;
        if (id != null && id.length() != 0) {
            result = restTemplate.put(URIWithDocId(id), entity, revisionHandler);
        } else {
            result = restTemplate.post(dbURI.toString(), entity, revisionHandler);
            Documents.setId(o, result.getId());
        }
        Documents.setRevision(o, result.getRevision());
    }

    @Override
    public void create(String id, Object node) {
        assertDocIdHasValue(id);
        Assert.notNull(node, "node may not be null");

        HttpEntity entity = createHttpEntity(node);

        restTemplate.put(URIWithDocId(id), entity);
    }

    @Override
    public PurgeResult purge(Map<String, List<String>> revisionsToPurge) {
        HttpEntity entity = createHttpEntity(revisionsToPurge);

        ResponseCallback<PurgeResult> responseCallback = getCouchDbConnectorResponseHandlerFactory().getClassInstanceResponseHandler(PurgeResult.class);

        return restTemplate.post(dbURI.append("_purge").toString(), entity, responseCallback);
    }

    @Override
    public void update(final Object o) {
        Assert.notNull(o, "Document cannot be null");
        final String id = Documents.getId(o);
        assertDocIdHasValue(id);

        HttpEntity entity = createHttpEntity(o);

        EntityUpdateResponseHandler responseHandler = getCouchDbConnectorResponseHandlerFactory().getEntityUpdateResponseHandler(o, id);
        restTemplate.put(dbURI.append(id).toString(), entity, responseHandler);
    }

}

