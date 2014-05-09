package org.ektorp;

/**
 * @author Henrik Lundgren
 *         created 18 okt 2009
 */
public class UpdateConflictException extends DbAccessException {

	private static final long serialVersionUID = 10910358334576950L;

	private final String docId;

	private final String revision;

	public UpdateConflictException(String message, String documentId, String revision) {
		super(message);
		this.docId = documentId;
		this.revision = revision;
	}

	public UpdateConflictException(String documentId, String revision) {
		this(null, documentId, revision);
	}

	public UpdateConflictException() {
		this("unknown", "unknown");
	}

	@Override
	public String getMessage() {
		String superMessage = super.getMessage();
		if (superMessage == null) {
			return String.format("document update conflict: id: %s rev: %s", docId, revision);
		} else {
			return String.format("document update conflict: id: %s rev: %s : %s", docId, revision, superMessage);
		}
	}

}
