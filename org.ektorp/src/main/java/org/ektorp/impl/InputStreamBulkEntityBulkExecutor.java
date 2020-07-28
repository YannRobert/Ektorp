package org.ektorp.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.ektorp.DocumentOperationResult;
import org.ektorp.http.InputStreamBulkEntity;
import org.ektorp.http.RestTemplate;
import org.ektorp.http.URI;

import java.io.InputStream;
import java.util.List;

public class InputStreamBulkEntityBulkExecutor implements BulkExecutor<InputStream> {

	protected RestTemplate restTemplate;

	protected ObjectMapper objectMapper;

	protected String bulkDocsUri;

	protected boolean requestCompressionEnabled = true;

	public InputStreamBulkEntityBulkExecutor(URI dbURI, RestTemplate restTemplate, ObjectMapper objectMapper) {
		this.restTemplate = restTemplate;
		this.objectMapper = objectMapper;
		this.bulkDocsUri = dbURI.append("_bulk_docs").toString();
	}

	@Override
	public List<DocumentOperationResult> executeBulk(InputStream inputStream, boolean allOrNothing) {
        HttpEntity httpEntity = new InputStreamBulkEntity(inputStream, allOrNothing);
		if (requestCompressionEnabled) {
			httpEntity = new GzipCompressingEntity(httpEntity);
		}
		return restTemplate.post(
				bulkDocsUri,
				httpEntity,
				new BulkOperationResponseHandler(objectMapper));
	}

	public void setRequestCompressionEnabled(boolean requestCompressionEnabled) {
		this.requestCompressionEnabled = requestCompressionEnabled;
	}

}
