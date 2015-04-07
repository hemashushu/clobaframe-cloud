package org.archboy.clobaframe.blobstore.amazons3;

import java.io.IOException;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import javax.inject.Inject;
import javax.inject.Named;
import org.springframework.util.Assert;
import org.archboy.clobaframe.blobstore.Blobstore;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import org.archboy.clobaframe.blobstore.BlobResourceRepository;
import org.archboy.clobaframe.blobstore.impl.AbstractBlobstore;

/**
 * {@link Blobstore} implements for Amazon S3.
 *
 * @author yang
 *
 */
@Named
public class S3Blobstore extends AbstractBlobstore {

	@Inject
	private S3ClientFactory clientFactory;

	private AmazonS3 client;

	@PostConstruct
	public void init(){
		client = clientFactory.getClient();
	}

	@Override
	public String getName() {
		return "amazons3";
	}

	@Override
	public boolean exist(String repoName) {
		Assert.hasText(repoName);
		return client.doesBucketExist(repoName);
	}

	@Override
	public void create(String repoName) throws IOException {
		Assert.hasText(repoName);

		String locationConstraint = clientFactory.getLocationConstraint();
		CreateBucketRequest request = new CreateBucketRequest(repoName);
		if (StringUtils.isNotEmpty(locationConstraint)){
			request.setRegion(locationConstraint);
		}

		try{
			client.createBucket(request);
		}catch(AmazonClientException e){
			throw new IOException(e);
		}
	}

	@Override
	public BlobResourceRepository getRepository(String repoName) {
		if (client.doesBucketExist(repoName)) {
			return new S3BlobResourceRepository(client, repoName);
		}else{
			return null;
		}
	}

	@Override
	public void delete(String repoName) throws IOException {
		Assert.hasText(repoName);
		
		try{
			client.deleteBucket(repoName);
		}catch (AmazonS3Exception e) {
			if(e.getStatusCode() == 404){
				// skip the non-exists repository.
				return;
			}
			throw new IOException(e);
		}
	}
}
