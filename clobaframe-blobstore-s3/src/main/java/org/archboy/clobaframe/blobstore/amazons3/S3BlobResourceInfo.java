package org.archboy.clobaframe.blobstore.amazons3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.archboy.clobaframe.blobstore.BlobResourceInfo;
import org.archboy.clobaframe.blobstore.impl.AbstractBlobResourceInfo;


/**
 * Translate Amazon S3 object into {@link BlobResourceInfo}.
 *
 * @author yang
 *
 */
public class S3BlobResourceInfo extends AbstractBlobResourceInfo {

	private String repositoryName;
	private String key;
	private ObjectMetadata objectMetadata;
	private AmazonS3 client;

	public S3BlobResourceInfo(String repositoryName, String key, 
			ObjectMetadata objectMetadata, AmazonS3 client) {
		this.repositoryName = repositoryName;
		this.key = key;
		this.objectMetadata = objectMetadata;
		this.client = client;
	}

	@Override
	public String getRepositoryName() {
		return repositoryName;
	}

	@Override
	public String getKey() {
		return key;
	}
	
	@Override
	public long getContentLength() {
		return objectMetadata.getContentLength();
	}

	@Override
	public String getMimeType() {
		return objectMetadata.getContentType();
	}

	@Override
	public Date getLastModified() {
		return objectMetadata.getLastModified();
	}

	@Override
	public InputStream getContent() throws IOException{
		try{
			S3Object s3Object = client.getObject(repositoryName, key);
			return s3Object.getObjectContent();

		}catch (AmazonS3Exception e) {
			if (e.getStatusCode() == 404){
				throw new FileNotFoundException(String.format(
						"Blob object [%s] not found in the amazon s3 repository [%s].",
						key, repositoryName));
			}else{
				throw new IOException(e);
			}
		}
	}

	@Override
	public InputStream getContent(long start, long length) throws IOException {
		try{
			GetObjectRequest request = new GetObjectRequest(repositoryName, key);

			request.setRange(start, start + length - 1); // both start and end byte are include.
			S3Object s3Object = client.getObject(request);

			return s3Object.getObjectContent();

		}catch (AmazonS3Exception e) {
			if (e.getStatusCode() == 404){
				throw new FileNotFoundException(String.format(
						"Blob object [%s] not found in the amazon s3 repository [%s].",
						key, repositoryName));
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
		return convertFromUserMetaData(objectMetadata.getUserMetadata());
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

}
