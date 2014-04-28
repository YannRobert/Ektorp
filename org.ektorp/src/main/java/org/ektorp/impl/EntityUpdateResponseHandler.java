package org.ektorp.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.ektorp.UpdateConflictException;
import org.ektorp.http.HttpResponse;
import org.ektorp.http.HttpStatus;
import org.ektorp.http.StdResponseHandler;
import org.ektorp.util.Documents;

import java.io.InputStream;

public class EntityUpdateResponseHandler extends StdResponseHandler<Void> {

	private ObjectMapper objectMapper;

	private Object entityObject;

	private String entityId;

	public EntityUpdateResponseHandler(ObjectMapper objectMapper) {
		super();
		this.objectMapper = objectMapper;
	}

	public EntityUpdateResponseHandler(ObjectMapper objectMapper, Object entityObject, String entityId) {
		super();
		this.objectMapper = objectMapper;
		this.entityObject = entityObject;
		this.entityId = entityId;
	}

	@Override
	public Void success(HttpResponse hr) throws Exception {
		final JsonNode node;

		InputStream content = null;
		try {
			content = hr.getContent();
			node = getObjectMapper().readValue(content, JsonNode.class);
		} finally {
			IOUtils.closeQuietly(content);
		}

		Documents.setRevision(entityObject, node.get("rev").textValue());
		return null;
	}

	@Override
	public Void error(HttpResponse hr) {
		if (hr.getCode() == HttpStatus.CONFLICT) {
			throw new UpdateConflictException(entityId, Documents.getRevision(entityObject));
		}
		return super.error(hr);
	}

	public void setEntityInfo(Object entityObject, String entityId) {
		this.entityObject = entityObject;
		this.entityId = entityId;
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}
}
