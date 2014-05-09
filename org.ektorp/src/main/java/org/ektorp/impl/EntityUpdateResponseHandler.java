package org.ektorp.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.ektorp.UpdateConflictException;
import org.ektorp.http.HttpResponse;
import org.ektorp.http.HttpStatus;
import org.ektorp.http.StdResponseHandler;
import org.ektorp.util.Documents;

import java.io.IOException;
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
			// we should consume the content of the response, even if we do not need it
			// the response should generally be this 58 chars long String, equals to : {"error":"conflict","reason":"Document update conflict."}\n
			String details;
			InputStream content = null;
			try {
				content = hr.getContent();
				details = IOUtils.toString(content);
			} catch (IOException e) {
				details = null;
			} finally {
				IOUtils.closeQuietly(content);
			}
			throw new UpdateConflictException(details, entityId, Documents.getRevision(entityObject));
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
