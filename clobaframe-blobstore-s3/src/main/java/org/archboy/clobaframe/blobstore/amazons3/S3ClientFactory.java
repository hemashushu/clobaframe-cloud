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

import java.io.IOException;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import java.io.File;
import java.io.FileNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Amazon S3 client factory.
 *
 * @author young
 *
 */
@Component
public class S3ClientFactory{

	private static final int DEFAULT_CONNECTION_TIMEOUT = 30 * 1000;
	private static final int DEFAULT_READ_TIMEOUT = 30 * 1000;

	private static final String DEFAULT_CREDENTIAL_FILE_NAME = "classpath:AwsCredentials.properties";

	@Value("${amazon.credentials.file}")
	private String credentialFilename = DEFAULT_CREDENTIAL_FILE_NAME;

	/**
	 * Amazon s3 region end endpoint, see:
	 * http://docs.amazonwebservices.com/general/latest/gr/rande.html#s3_region
	 *
	 */
	@Value("${blobstore.amazons3.endPoint}")
	private String endPoint;

	/**
	 * The location constraint of region, see:
	 * http://docs.amazonwebservices.com/general/latest/gr/rande.html#s3_region
	 *
	 */
	@Value("${blobstore.amazons3.locationConstraint}")
	private String locationConstraint;

	@Value("${blobstore.amazons3.secureConnection}")
	private boolean secureConnection;

	@Autowired
	private ResourceLoader resourceLoader;

	private AmazonS3 client;

	private Logger logger = LoggerFactory.getLogger(S3ClientFactory.class);

	@PostConstruct
	public void init() throws IOException {

		Resource resource = resourceLoader.getResource(credentialFilename);
		File file = resource.getFile();
		if (!file.exists()){
			logger.error("Current default path is [{}], can not find the file [{}].",
					resourceLoader.getResource(".").getFile().getAbsolutePath(),
					credentialFilename);
			throw new FileNotFoundException();
		}

		AWSCredentials credentials = new PropertiesCredentials(file);
		client = new AmazonS3Client(
				credentials,
				createConfiguration(secureConnection));
		client.setEndpoint(endPoint);
	}

	public AmazonS3 getClient() {
		return client;
	}

	public String getLocationConstraint() {
		return locationConstraint;
	}

    private ClientConfiguration createConfiguration(boolean isSecure) {
        ClientConfiguration config = new ClientConfiguration();
        Protocol protocol = isSecure ? Protocol.HTTPS : Protocol.HTTP;
        config.setProtocol(protocol);

		String connectTimeout = System.getProperty("sun.net.client.defaultConnectTimeout",
				new Integer(DEFAULT_CONNECTION_TIMEOUT).toString());
		String readTimeout = System.getProperty("sun.net.client.defaultReadTimeout",
				new Integer(DEFAULT_READ_TIMEOUT).toString());

		config.setConnectionTimeout(Integer.parseInt(connectTimeout));
		config.setSocketTimeout(Integer.parseInt(readTimeout));

        return config;
    }
}
