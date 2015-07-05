package org.archboy.clobaframe.blobstore.amazons3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

/**
 * Amazon S3 client factory.
 *
 * @author yang
 *
 */
@Named
public class S3ClientFactory implements ResourceLoaderAware, InitializingBean {

	private static final int DEFAULT_CONNECTION_TIMEOUT = 30 * 1000;
	private static final int DEFAULT_READ_TIMEOUT = 30 * 1000;

	private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
	private int readTimeout = DEFAULT_READ_TIMEOUT;
	
	private static final String DEFAULT_CREDENTIAL_FILE_NAME = ""; //"classpath:AwsCredentials.properties";
	private static final String DEFAULT_ENDPOINT = ""; //"s3.amazonaws.com";
	private static final String DEFAULT_LOCATION_CONSTRAINT = "";
	private static final boolean DEFAULT_SECURE_CONNECTION = true;
	
	@Value("${clobaframe.amazon.credentials.file:" + DEFAULT_CREDENTIAL_FILE_NAME + "}")
	private String credentialFilename;

	/**
	 * Amazon s3 region end endpoint, see:
	 * http://docs.amazonwebservices.com/general/latest/gr/rande.html#s3_region
	 *
	 */
	@Value("${clobaframe.blobstore.amazons3.endPoint:" + DEFAULT_ENDPOINT + "}")
	private String endPoint;

	/**
	 * The location constraint of region, see:
	 * http://docs.amazonwebservices.com/general/latest/gr/rande.html#s3_region
	 *
	 */
	@Value("${clobaframe.blobstore.amazons3.locationConstraint:" + DEFAULT_LOCATION_CONSTRAINT + "}")
	private String locationConstraint;

	@Value("${clobaframe.blobstore.amazons3.secureConnection:" + DEFAULT_SECURE_CONNECTION + "}")
	private boolean secureConnection;

	//@Inject
	private ResourceLoader resourceLoader;

	private AmazonS3 client;

	private Logger logger = LoggerFactory.getLogger(S3ClientFactory.class);

	@Override
	public void setResourceLoader(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	public void setCredentialFilename(String credentialFilename) {
		this.credentialFilename = credentialFilename;
	}

	public void setEndPoint(String endPoint) {
		this.endPoint = endPoint;
	}

	public void setLocationConstraint(String locationConstraint) {
		this.locationConstraint = locationConstraint;
	}

	public void setSecureConnection(boolean secureConnection) {
		this.secureConnection = secureConnection;
	}

	//@PostConstruct
	@Override
	public void afterPropertiesSet() throws Exception {

		if (StringUtils.isEmpty(credentialFilename) ||
				StringUtils.isEmpty(endPoint)) {
			return;
		}
		
		Resource resource = resourceLoader.getResource(credentialFilename);
		if (!resource.exists()){
			throw new FileNotFoundException(
					String.format(
							"Can not find the amazon web service credential file [{}].",
							credentialFilename));
		}

		InputStream in = null;
		
		try {
			in =resource.getInputStream();
			AWSCredentials credentials = new PropertiesCredentials(in);

			client = new AmazonS3Client(
					credentials,
					createAWSClientConfiguration(secureConnection));

			client.setEndpoint(endPoint);
		}finally{
			IOUtils.closeQuietly(in);
		}
	}

	public AmazonS3 getClient() {
		return client;
	}

	public String getLocationConstraint() {
		return locationConstraint;
	}

    private ClientConfiguration createAWSClientConfiguration(boolean isSecure) {
        
		ClientConfiguration config = new ClientConfiguration();
        Protocol protocol = isSecure ? Protocol.HTTPS : Protocol.HTTP;
        config.setProtocol(protocol);

		int cTimeout = Integer.parseInt(
				System.getProperty("sun.net.client.defaultConnectTimeout",
					Integer.toString(connectionTimeout)));
		
		int rTimeout = Integer.parseInt(
				System.getProperty("sun.net.client.defaultReadTimeout",
					Integer.toString(readTimeout)));

		config.setConnectionTimeout(cTimeout);
		config.setSocketTimeout(rTimeout);

        return config;
    }
}
