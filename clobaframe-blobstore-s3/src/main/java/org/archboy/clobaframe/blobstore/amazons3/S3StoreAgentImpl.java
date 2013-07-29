/*
 * Copyright 2011 Spark Young (sparkyoungs@gmail.com). All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.archboy.clobaframe.blobstore.amazons3;

import java.io.FileNotFoundException;
import java.io.IOException;
import javax.annotation.PostConstruct;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.archboy.clobaframe.blobstore.BlobInfo;
import org.archboy.clobaframe.blobstore.BlobInfoPartialCollection;
import org.archboy.clobaframe.blobstore.BlobKey;
import org.archboy.clobaframe.blobstore.Blobstore;
import org.archboy.clobaframe.blobstore.StoreAgent;
import org.archboy.clobaframe.webio.ResourceContent;
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

/**
 * {@link Blobstore} implements for Amazon S3.
 *
 * @author young
 *
 */
@Component
public class S3StoreAgentImpl implements StoreAgent {

	@Autowired
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
	public boolean existBucket(String name) {
		return client.doesBucketExist(name);
	}

	@Override
	public void createBucket(String name) throws IOException{
		Assert.hasText(name);

		String locationConstraint = clientFactory.getLocationConstraint();

		CreateBucketRequest request = new CreateBucketRequest(name);
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
	public void deleteBucket(String name) throws IOException {
		try{
			client.deleteBucket(name);
		}catch (AmazonS3Exception e) {
			if(e.getStatusCode() == 404){
				// skip the non-exists blob.
				return;
			}
			throw new IOException(e);
		}
	}

	@Override
	public void put(BlobInfo blobInfo, boolean publicReadable, boolean minor) throws IOException {
		Assert.notNull(blobInfo);

		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(blobInfo.getContentLength());
		meta.setContentType(blobInfo.getContentType());
		meta.setUserMetadata(blobInfo.getMetadata());

		BlobKey blobKey = blobInfo.getBlobKey();
		ResourceContent resourceContent = blobInfo.getContentSnapshot();

		PutObjectRequest request = new PutObjectRequest(
				blobKey.getBucketName(),
				blobKey.getKey(),
				resourceContent.getInputStream(),
				meta);

		if (publicReadable) {
			request.setCannedAcl(CannedAccessControlList.PublicRead);
		}

		if (minor) {
			request.setStorageClass(StorageClass.ReducedRedundancy);
		}

		try{
			client.putObject(request);
		}catch(AmazonClientException e){
			throw new IOException(e);
		}finally{
			IOUtils.closeQuietly(resourceContent);
		}
	}

	@Override
	public BlobInfo get(BlobKey blobKey) throws IOException {
		Assert.notNull(blobKey);

		try{
			ObjectMetadata objectMetadata = client.getObjectMetadata(
					blobKey.getBucketName(),
					blobKey.getKey());
			return new BlobInfoFromS3Object(blobKey, objectMetadata, client);
		}catch (AmazonS3Exception e) {
			if (e.getStatusCode() == 404){
				throw new FileNotFoundException(String.format(
						"Blob [%s] not found in the bucket [%s].",
						blobKey.getKey(), blobKey.getBucketName()));
			}else{
				throw new IOException(e);
			}
		}
	}

	@Override
	public void delete(BlobKey blobKey) throws IOException {
		Assert.notNull(blobKey);

		try{
			client.deleteObject(
					blobKey.getBucketName(),
					blobKey.getKey());
		}catch (AmazonS3Exception e) {
			if(e.getStatusCode() == 404){
				// skip the non-exists blob.
				return;
			}
			throw new IOException(e);
		}
	}

	@Override
	public BlobInfoPartialCollection list(BlobKey prefix) {
		Assert.notNull(prefix);

		ObjectListing objectListing = null;
		if (StringUtils.isEmpty(prefix.getKey())) {
			objectListing = client.listObjects(prefix.getBucketName());
		} else {
			objectListing = client.listObjects(prefix.getBucketName(), prefix.getKey());
		}

		return convertToCollection(objectListing);
	}

	@Override
	public BlobInfoPartialCollection listNext(BlobInfoPartialCollection collection) {
		Assert.isInstanceOf(S3BlobInfoPartialCollection.class, collection);

		ObjectListing oldObjectListing = ((S3BlobInfoPartialCollection)collection).getObjectListing();
		ObjectListing objectListing = client.listNextBatchOfObjects(oldObjectListing);
		return convertToCollection(objectListing);
	}

	private BlobInfoPartialCollection convertToCollection(ObjectListing objectListing) {
		S3BlobInfoPartialCollection collection = new S3BlobInfoPartialCollection(objectListing);
		for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
			collection.add(new BlobInfoFromS3ObjectSummary(summary, client));
		}

		collection.setHasMore(objectListing.isTruncated());
		return collection;
	}
}
