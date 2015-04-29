package org.archboy.clobaframe.blobstore.amazons3;

import com.amazonaws.services.s3.model.ObjectListing;
import java.util.ArrayList;
import java.util.Collection;
import org.archboy.clobaframe.blobstore.PartialCollection;

/**
 *
 * @author yang
 */
public class S3PartialArrayList<T>
		extends ArrayList<T>
		implements PartialCollection<T> {

	private static final long serialVersionUID = 1L;
	
	private ObjectListing objectListing;
	private boolean hasMore;

	public S3PartialArrayList(Collection<? extends T> c, ObjectListing objectListing, boolean hasMore) {
		super(c);
		this.objectListing = objectListing;
		this.hasMore = hasMore;
	}

	@Override
	public boolean hasMore() {
		return hasMore;
	}

	public ObjectListing getObjectListing() {
		return objectListing;
	}
}