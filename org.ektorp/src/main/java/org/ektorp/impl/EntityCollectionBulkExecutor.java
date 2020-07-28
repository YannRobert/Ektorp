package org.ektorp.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.ektorp.DocumentOperationResult;
import org.ektorp.http.JacksonableEntity;
import org.ektorp.http.RestTemplate;
import org.ektorp.http.URI;

import java.util.Collection;
import java.util.List;

/**
 * This is designed to replace the BulkOperationCollectionBulkExecutor
 */
public class EntityCollectionBulkExecutor implements BulkExecutor<Collection<?>> {

    protected URI dbURI;

    protected RestTemplate restTemplate;

    protected ObjectMapper objectMapper;
    
    protected boolean requestCompressionEnabled = true;

    public EntityCollectionBulkExecutor() {

    }

    public EntityCollectionBulkExecutor(URI dbURI, RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.dbURI = dbURI;
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public List<DocumentOperationResult> executeBulk(Collection<?> objects, boolean allOrNothing) {
        BulkDocumentBean<?> bulkDocumentBean = new BulkDocumentBean(objects, allOrNothing);
        return restTemplate.post(
                dbURI.append("_bulk_docs").toString(),
                createHttpEntity(bulkDocumentBean),
                new BulkOperationResponseHandler(objects, objectMapper));
    }

    protected HttpEntity createHttpEntity(Object o) {
        JacksonableEntity jacksonableEntity = new JacksonableEntity(o, objectMapper);
        if (requestCompressionEnabled) {
            return new GzipCompressingEntity(jacksonableEntity);
        }
        return jacksonableEntity;
    }
    
    public void setRequestCompressionEnabled(boolean requestCompressionEnabled) {
        this.requestCompressionEnabled = requestCompressionEnabled;
    }

}
