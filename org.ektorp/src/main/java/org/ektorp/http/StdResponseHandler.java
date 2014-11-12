package org.ektorp.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.ektorp.DbAccessException;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.UpdateConflictException;

import java.io.IOException;
import java.io.InputStream;

/**
 * @param <T>
 * @author henrik lundgren
 */
public class StdResponseHandler<T> implements ResponseCallback<T> {

    private final static ObjectMapper MAPPER = new ObjectMapper();

	public static void consumeInputStreamQuietly(HttpResponse hr) {
		InputStream content = null;
		try {
			content = hr.getContent();
			if (content != null) {
				// this will consume the content
				try {
					IOUtils.copy(content, NullOutputStream.NULL_OUTPUT_STREAM);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		} finally {
			IOUtils.closeQuietly(content);
		}
	}

    /**
     * Creates an DbAccessException which specific type is determined by the response code in the http response.
     */
    public static DbAccessException createDbAccessException(HttpResponse hr) {
        return createDbAccessException(hr, MAPPER);
    }

    public static DbAccessException createDbAccessException(HttpResponse hr, ObjectMapper mapper) {
        JsonNode responseBody;

        InputStream content = null;
        try {
            content = hr.getContent();
            if (content != null) {
                responseBody = responseBodyAsNode(IOUtils.toString(content), mapper);
            } else {
                responseBody = NullNode.getInstance();
            }
        } catch (IOException e) {
            responseBody = NullNode.getInstance();
        } finally {
            IOUtils.closeQuietly(content);
        }

        switch (hr.getCode()) {
            case HttpStatus.NOT_FOUND:
                return new DocumentNotFoundException(hr.getRequestURI(), responseBody);
            case HttpStatus.CONFLICT:
                return new UpdateConflictException();
            default:
                String body;
                try {
                    body = toPrettyString(responseBody);
                } catch (IOException e) {
                    body = "unavailable";
                }
                return new DbAccessException(hr.toString() + "\nURI: " + hr.getRequestURI() + "\nResponse Body: \n" + body);
        }
    }

    private static String toPrettyString(JsonNode n) throws IOException {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(n);
    }

    private static JsonNode responseBodyAsNode(String s, ObjectMapper mapper) throws IOException {
        if (s == null || s.length() == 0) {
            return NullNode.getInstance();
        } else if (!s.startsWith("{")) {
            return NullNode.getInstance();
        }
        return mapper.readTree(s);
    }

    protected static JsonNode responseBodyAsNode(InputStream inputStream, ObjectMapper mapper) throws IOException {
		return mapper.readTree(inputStream);
	}

	protected static <T> T checkResponseBodyOkAndReturnDefaultValue(HttpResponse hr, T defaultValue, ObjectMapper mapper) throws IOException {
		InputStream content = hr.getContent();
		try {
			content = hr.getContent();
			JsonNode body = responseBodyAsNode(content, MAPPER);
			JsonNode okNode = body.get("ok");
			if (okNode != null) {
				if (okNode.isBoolean()) {
                    if (okNode.booleanValue()) {
						return defaultValue;
					}
				}
			}
			throw new DbAccessException("Unexpected response body content, expected {\"ok\":true}, got " + body.toString());
		} finally {
			IOUtils.closeQuietly(content);
		}
	}

    public T error(HttpResponse hr) {
        throw createDbAccessException(hr);
    }

    public T success(HttpResponse hr) throws Exception {
		return checkResponseBodyOkAndReturnDefaultValue(hr, null, MAPPER);
    }

}
