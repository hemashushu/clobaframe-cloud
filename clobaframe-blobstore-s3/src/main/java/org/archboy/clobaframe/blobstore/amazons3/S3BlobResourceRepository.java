package org.archboy.clobaframe.blobstore.amazons3;

import java.io.FileNotFoundException;
import java.io.IOException;
import javax.annotation.PostConstruct;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import javax.inject.Inject;
import javax.inject.Named;
import org.springframework.util.Assert;
import org.archboy.clobaframe.blobstore.BlobResourceInfo;
import org.archboy.clobaframe.blobstore.Blobstore;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.archboy.clobaframe.blobstore.BlobResourceRepository;
import org.archboy.clobaframe.blobstore.PartialCollection;
import org.archboy.clobaframe.blobstore.impl.AbstractBlobResourceRepository;
import org.archboy.clobaframe.blobstore.impl.AbstractBlobstore;

/**
 * {@link BlobResourceRepository} implements for Amazon S3.
 *
 * @author yang
 *
 */
public class S3BlobResourceRepository extends AbstractBlobResourceRepository {

	private AmazonS3 client;
	private String name; // repo name

	public S3BlobResourceRepository(AmazonS3 client, String name) {
		this.client = client;
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}
	
	@Override
	public void put(BlobResourceInfo blobResourceInfo, boolean publicReadable, int priority) throws IOException {
		Assert.notNull(blobResourceInfo);

		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(blobResourceInfo.getContentLength());
		meta.setContentType(blobResourceInfo.getMimeType());
		meta.setUserMetadata(convertToUserMetaData(blobResourceInfo.getMetadata()));

		InputStream in = blobResourceInfo.getContent();

		PutObjectRequest request = new PutObjectRequest(
				name,
				blobResourceInfo.getKey(),
				in,
				meta);

		if (publicReadable) {
			request.setCannedAcl(CannedAccessControlList.PublicRead);
		}

		if (priority == PRIORITY_MIN) {
			request.setStorageClass(StorageClass.ReducedRedundancy);
		}

		try{
			client.putObject(request);
		}catch(AmazonClientException e){
			throw new IOException(e);
		}finally{
			IOUtils.closeQuietly(in);
		}
	}

	@Override
	public BlobResourceInfo get(String key) {
		Assert.notNull(key);

		try{
			ObjectMetadata objectMetadata = client.getObjectMetadata(name, key);
			return new S3BlobResourceInfo(name, key, objectMetadata, client);
		}catch (AmazonS3Exception e) {
			if (e.getStatusCode() == 404){
//				throw new FileNotFoundException(String.format(
//						"Blob [%s] not found in the bucket [%s].",
//						blobKey.getKey(), blobKey.getBucketName()));
				return null;
			}else{
				throw e;
			}
		}
	}

	@Override
	public void delete(String key) throws IOException {
		Assert.notNull(key);

		try{
			client.deleteObject(name, key);
		}catch (AmazonS3Exception e) {
			if(e.getStatusCode() == 404){
				// ignore the non-exists blob object.
				return;
			}
			throw new IOException(e);
		}
	}

	@Override
	public PartialCollection<BlobResourceInfo> list() {
		ObjectListing objectListing = client.listObjects(name);
		return convertToPartialCollection(objectListing);
	}

	@Override
	public PartialCollection<BlobResourceInfo> listNext(PartialCollection<BlobResourceInfo> prevCollection) {
		Assert.isInstanceOf(S3PartialArrayList.class, prevCollection);

		ObjectListing oldObjectListing = ((S3PartialArrayList)prevCollection).getObjectListing();
		ObjectListing objectListing = client.listNextBatchOfObjects(oldObjectListing);
		return convertToPartialCollection(objectListing);
	}

	private S3PartialArrayList convertToPartialCollection(ObjectListing objectListing) {
		Assert.notNull(objectListing);
		
		List<BlobResourceInfo> items = new ArrayList<BlobResourceInfo>();
		for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
			items.add(new S3BlobResourceInfoBySummary(summary, client));
		}

		return new S3PartialArrayList(items, objectListing, objectListing.isTruncated());
	}
	
	private Map<String, String> convertToUserMetaData(Map<String, Object> source){
		if (source == null || source.isEmpty()) {
			return null;
		}
		
		Map<String, String> meta = new HashMap<String, String>();
		for(Map.Entry<String, Object> entry : source.entrySet()){
			meta.put(entry.getKey(), entry.getValue().toString());
		}
		
		return meta;
	}
}
