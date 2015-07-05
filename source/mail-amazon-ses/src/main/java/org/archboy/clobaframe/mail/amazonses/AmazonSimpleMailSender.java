package org.archboy.clobaframe.mail.amazonses;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.inject.Inject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import javax.inject.Named;
import org.archboy.clobaframe.mail.SendMailException;
import org.archboy.clobaframe.mail.impl.AbstractMailSender;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ResourceLoaderAware;

/**
 * Send mail by Amazon SNS service.
 *
 * @author yang
 */
@Named
public class AmazonSimpleMailSender extends AbstractMailSender 
	implements ResourceLoaderAware, InitializingBean {

	private static final String DEFAULT_CREDENTIAL_FILE_NAME = ""; // "classpath:AwsCredentials.properties";
	private static final String DEFAULT_SENDER_ADDRESS = "";
	
	@Value("${clobaframe.amazon.credentials.file:" + DEFAULT_CREDENTIAL_FILE_NAME + "}")
	private String credentialFilename;

	@Value("${clobaframe.mail.amazonses.fromAddress" + DEFAULT_SENDER_ADDRESS + "}")
	private String fromAddress; 

	//@Inject
	private ResourceLoader resourceLoader;

	private AmazonSimpleEmailServiceClient client;
	
	private Logger logger = LoggerFactory.getLogger(AmazonSimpleMailSender.class);

	@Override
	public void setResourceLoader(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	public void setCredentialFilename(String credentialFilename) {
		this.credentialFilename = credentialFilename;
	}

	public void setFromAddress(String fromAddress) {
		this.fromAddress = fromAddress;
	}

	//@PostConstruct
	@Override
	public void afterPropertiesSet() throws Exception {

		if (StringUtils.isEmpty(credentialFilename) ||
				StringUtils.isEmpty(fromAddress)){
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
		try{
			in = resource.getInputStream();
			AWSCredentials credentials = new PropertiesCredentials(in);
			IOUtils.closeQuietly(in);

			// Set AWS access credentials
			client = new AmazonSimpleEmailServiceClient(credentials)
					.withEndpoint("email.us-east-1.amazonaws.com");
//		}catch(IOException e){
//			logger.error("can not load amazon simple email service.");
		}finally{
			IOUtils.closeQuietly(in);
		}
	}

	@Override
	public String getName() {
		return "amazonses";
	}

	@Override
	public void send(String recipient, String subject, String content) throws SendMailException {
		SendEmailRequest request = new SendEmailRequest()
				.withSource(fromAddress);

//		List<String> toAddresses = new ArrayList<String>();
//		toAddresses.add(recipient);
		List<String> toAddresses = Arrays.asList(recipient);
		Destination dest = new Destination()
				.withToAddresses(toAddresses);
		
		request.setDestination(dest);

		Content subjectContent = new Content()
				.withData(subject);
		
		Message message = new Message()
				.withSubject(subjectContent);

		// Include a body in both text and HTML formats
		Content textContent = new Content()
				.withCharset("UTF-8") // specify the charset.
				.withData(content);
		
		//Content htmlContent = new Content().withData("");

		Body body = new Body()
				.withText(textContent);
				//.withHtml(htmlContent);
		message.setBody(body);

		request.setMessage(message);

		// Call Amazon SES to send the message
		//try {
			client.sendEmail(request);
//		} catch (AmazonClientException e) {
//			throw new SendMailException(
//					String.format("Failed to send mail to %s.", recipient), e);
//		}
	}

	@Override
	public void sendWithHtml(String recipient, String subject, String content) throws SendMailException {
		SendEmailRequest request = new SendEmailRequest()
				.withSource(fromAddress);

		//List<String> toAddresses = new ArrayList<String>();
		//toAddresses.add(recipient);
		List<String> toAddresses = Arrays.asList(recipient);
		Destination dest = new Destination().withToAddresses(toAddresses);
		request.setDestination(dest);

		Content subjectContent = new Content()
				.withData(subject);
		
		Message message = new Message()
				.withSubject(subjectContent);

		// Include a body in both text and HTML formats
		//Content textContent = new Content().withData(content);
		Content htmlContent = new Content()
				.withData(content);

		Body body = new Body()
				//.withText(textContent);
				.withHtml(htmlContent);

		message.setBody(body);

		request.setMessage(message);

		// Call Amazon SES to send the message
		try {
			client.sendEmail(request);
		} catch (AmazonClientException e) {
			throw new SendMailException(
					String.format("Failed to send mail to %s.", recipient), e);
		}
	}
}
