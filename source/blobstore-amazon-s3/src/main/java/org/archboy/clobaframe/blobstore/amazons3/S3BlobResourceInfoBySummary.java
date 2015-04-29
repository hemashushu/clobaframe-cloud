package org.archboy.clobaframe.blobstore.amazons3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import org.archboy.clobaframe.blobstore.BlobResourceInfo;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.InputStream;
import java.util.HashMap;
import org.archboy.clobaframe.blobstore.impl.AbstractBlobResourceInfo;

/**
 * Translate Amazon S3 object into {@link BlobResourceInfo} with lazy load.
 *
 * @author yang
 *
 */
public class S3BlobResourceInfoBySummary extends AbstractBlobResourceInfo {

	private S3ObjectSummary summary;
	private AmazonS3 client;

	private ObjectMetadata objectMetadata; // the cache object
	
	public S3BlobResourceInfoBySummary(S3ObjectSummary summary, AmazonS3 client) {
		this.summary = summary;
		this.client = client;
	}

	@Override
	public String getRepositoryName() {
		return summary.getBucketName();
	}

	@Override
	public String getKey() {
		return summary.getKey();
	}
	
	@Override
	public long getContentLength() {
		return summary.getSize();
	}

	@Override
	public String getMimeType() {
		return getObjectMetadata().getContentType();
	}

	@Override
	public Date getLastModified() {
		return summary.getLastModified();
	}

	@Override
	public InputStream getContent() throws IOException{
		try{
			S3Object s3Object = client.getObject(
					summary.getBucketName(),
					summary.getKey());

			return s3Object.getObjectContent();

		}catch (AmazonS3Exception e) {
			if (e.getStatusCode() == 404){
				throw new FileNotFoundException(String.format(
						"Blob object [%s] not found in the amazon s3 repository [%s].",
						summary.getKey(), summary.getBucketName()));
			}else{
				throw new IOException(e);
			}
		}
	}

	@Override
	public InputStream getContent(long start, long length) throws IOException{
		try{
			GetObjectRequest request = new GetObjectRequest(
					summary.getBucketName(),
					summary.getKey());

			request.setRange(start, start + length -1); // both start and end byte are include.
			S3Object s3Object = client.getObject(request);

			return s3Object.getObjectContent();

		}catch (AmazonS3Exception e) {
			if (e.getStatusCode() == 404){
				throw new FileNotFoundException(String.format(
						"Blob object [%s] not found in the amazon s3 repository [%s].",
						summary.getKey(), summary.getBucketName()));
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
	public Map<String, Object> getMetadata() {
		return convertFromUserMetaData(getObjectMetadata().getUserMetadata());
	}
	
	private Map<String, Object> convertFromUserMetaData(Map<String, String> source){
		if (source == null || source.isEmpty()) {
			return null;
		}
		
		Map<String, Object> meta = new HashMap<String, Object>();
		for(Map.Entry<String, String> entry : source.entrySet()){
			meta.put(entry.getKey(), entry.getValue());
		}
		
		return meta;
	}

	private synchronized ObjectMetadata getObjectMetadata() {
		if (objectMetadata == null){
			objectMetadata = client.getObjectMetadata(
				summary.getBucketName(),
				summary.getKey());
		}
		return objectMetadata;
	}
}
