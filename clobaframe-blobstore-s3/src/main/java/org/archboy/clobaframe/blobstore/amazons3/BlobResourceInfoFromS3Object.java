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
import java.util.Date;
import java.util.Map;
import org.archboy.clobaframe.blobstore.BlobResourceInfo;
import org.archboy.clobaframe.blobstore.BlobKey;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import java.io.InputStream;


/**
 * Translate Amazon S3 object into {@link BlobResourceInfo}.
 *
 * @author young
 *
 */
public class BlobResourceInfoFromS3Object implements BlobResourceInfo {

	private BlobKey blobKey;
	private ObjectMetadata objectMetadata;
	private AmazonS3 client;

	public BlobResourceInfoFromS3Object(BlobKey blobKey, ObjectMetadata objectMetadata, AmazonS3 client) {
		this.blobKey = blobKey;
		this.objectMetadata = objectMetadata;
		this.client = client;
	}

	@Override
	public BlobKey getBlobKey() {
		return blobKey;
	}

	@Override
	public long getContentLength() {
		return objectMetadata.getContentLength();
	}

	@Override
	public String getContentType() {
		return objectMetadata.getContentType();
	}

	@Override
	public Date getLastModified() {
		return objectMetadata.getLastModified();
	}

	@Override
	public InputStream getInputStream() throws IOException{
		try{
			S3Object s3Object = client.getObject(
					blobKey.getBucketName(),
					blobKey.getKey());

			return s3Object.getObjectContent();

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
	public InputStream getInputStream(long start, long length) throws IOException {
		try{
			GetObjectRequest request = new GetObjectRequest(
					blobKey.getBucketName(),
					blobKey.getKey());

			request.setRange(start, start + length - 1); // both start and end byte are include.
			S3Object s3Object = client.getObject(request);

			return s3Object.getObjectContent();

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
	public boolean isSeekable() {
		return true;
	}

	@Override
	public Map<String, String> getMetadata() {
		return objectMetadata.getUserMetadata();
	}

	@Override
	public void addMetadata(String key, String value) {
		objectMetadata.addUserMetadata(key, value);
	}

}
