package org.archboy.clobaframe.blobstore.amazons3;

import org.archboy.clobaframe.blobstore.impl.AbstractBlobInfoPartialCollection;
import com.amazonaws.services.s3.model.ObjectListing;

/**
 *
 * @author arch
 */
public class S3BlobInfoPartialCollection extends AbstractBlobInfoPartialCollection {

	private static final long serialVersionUID = 1L;

	private ObjectListing objectListing;

	public S3BlobInfoPartialCollection(ObjectListing objectListing) {
		this.objectListing = objectListing;
	}

	public ObjectListing getObjectListing() {
		return objectListing;
	}
}
