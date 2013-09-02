package org.archboy.clobaframe.blobstore.amazons3;

import org.archboy.clobaframe.blobstore.impl.AbstractBlobResourceInfoPartialCollection;
import com.amazonaws.services.s3.model.ObjectListing;

/**
 *
 * @author arch
 */
public class S3BlobResourceInfoPartialCollection extends AbstractBlobResourceInfoPartialCollection {

	private static final long serialVersionUID = 1L;

	private ObjectListing objectListing;

	public S3BlobResourceInfoPartialCollection(ObjectListing objectListing) {
		this.objectListing = objectListing;
	}

	public ObjectListing getObjectListing() {
		return objectListing;
	}
}
