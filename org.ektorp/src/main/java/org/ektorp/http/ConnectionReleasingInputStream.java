package org.ektorp.http;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class ConnectionReleasingInputStream extends InputStream {

	private final static Logger LOG = LoggerFactory.getLogger(ConnectionReleasingInputStream.class);

	private final Throwable instantiationStackTrace = new Throwable("This is the instantiation stack trace of the ConnectionReleasingInputStream instance");

	private boolean closed = false;

	private boolean eof = false;

	private final HttpResponse httpResponse;

	private final InputStream in;

	// visible for tests
	public ConnectionReleasingInputStream(InputStream src, HttpResponse httpResponse) {
		super();
		this.in = src;
		this.httpResponse = httpResponse;
		if (src == null) {
			eof = true;
		}
	}

	@Override
	public int read() throws IOException {
		if (in == null) {
			return -1;
		}
		int read = in.read();
		eof = (read == -1);
		return read;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (len == 0) {
			return 0;
		}
		if (in == null) {
			return -1;
		}
		int read = in.read(b, off, len);
		eof = ((read == -1) || (read < len));
		return read;
	}

	@Override
	public void close() throws IOException {
		try {
			consumeContentIfNeeded();
		} finally {
			closeInnerInputStream();
		}
		if (httpResponse != null) {
			httpResponse.releaseConnection();
		}
	}

	public void consumeContentIfNeeded() throws IOException {
		if (!closed) {
			if (!eof) {
				if (in != null) {
					consumeContent();
				}
			}
		}
	}

	public void consumeContent() throws IOException {
		// this will consume the content
		int unconsumedLength = IOUtils.copy(this, NullOutputStream.NULL_OUTPUT_STREAM);
		if (unconsumedLength > 0) {
			String warningMessage = "content was not consumed entirely by the application. Make sure you consume the content entirely before closing it : " + unconsumedLength;
			LOG.warn(warningMessage, new RuntimeException(warningMessage));
		}
	}

	public void closeInnerInputStream() {
		IOUtils.closeQuietly(in);
		closed = true;
	}

	public boolean isClosed() {
		return closed;
	}

	@Override
	protected void finalize() {
		if (!closed) {
			LOG.warn("ConnectionReleasingInputStream was not closed properly. In order to avoid leaking connections, don't forget to call close() on every instance of InputStream retrieved on the StdHttpResponse", instantiationStackTrace);
		}
	}


}
