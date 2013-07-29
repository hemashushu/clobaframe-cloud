package org.archboy.clobaframe.mail.amazonses;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import org.archboy.clobaframe.mail.SendMailException;
import org.archboy.clobaframe.mail.SenderAgent;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;

/**
 * Send mail by Amazon SNS service.
 *
 * @author arch
 */
@Component
public class AmazonMailSenderAgent implements SenderAgent{

	private static final String AGENT_NAME = "amazonses";

	private static final String DEFAULT_CREDENTIAL_FILE_NAME = "classpath:AwsCredentials.properties";

	@Value("${amazon.credentials.file}")
	private String credentialFilename = DEFAULT_CREDENTIAL_FILE_NAME;

	@Value("${mail.amazonses.fromAddress}")
	private String fromAddress; //"noreply@tapedock.com"

	@Autowired
	private ResourceLoader resourceLoader;

	private AmazonSimpleEmailServiceClient client;
	private Logger logger = LoggerFactory.getLogger(AmazonMailSenderAgent.class);

	@PostConstruct
	public void init() throws IOException{
		Resource resource = resourceLoader.getResource(credentialFilename);
		File file = resource.getFile();
		if (!file.exists()){
			logger.error("Current default path is [{}], can not find the file [{}].",
					resourceLoader.getResource(".").getFile().getAbsolutePath(),
					credentialFilename);
			throw new FileNotFoundException();
		}

		AWSCredentials credentials = new PropertiesCredentials(file);

		// Set AWS access credentials
		client = new AmazonSimpleEmailServiceClient(credentials);
	}

	@Override
	public String getName() {
		return AGENT_NAME;
	}

	@Override
	public void send(String recipient, String subject, String content) throws SendMailException {
		SendEmailRequest request = new SendEmailRequest()
				.withSource(fromAddress);

		List<String> toAddresses = new ArrayList<String>();
		toAddresses.add(recipient);
		Destination dest = new Destination().withToAddresses(toAddresses);
		request.setDestination(dest);

		Content subjectContent = new Content().withData(subject);
		Message message = new Message().withSubject(subjectContent);

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
		try {
			client.sendEmail(request);
		} catch (AmazonClientException e) {
			throw new SendMailException(
					String.format("Failed to send mail to %s.", recipient));
		}
	}

	@Override
	public void sendWithHtml(String recipient, String subject, String content) throws SendMailException {
		SendEmailRequest request = new SendEmailRequest()
				.withSource(fromAddress);

		List<String> toAddresses = new ArrayList<String>();
		toAddresses.add(recipient);
		Destination dest = new Destination().withToAddresses(toAddresses);
		request.setDestination(dest);

		Content subjectContent = new Content().withData(subject);
		Message message = new Message().withSubject(subjectContent);

		// Include a body in both text and HTML formats
		//Content textContent = new Content().withData(content);
		Content htmlContent = new Content().withData(content);

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
					String.format("Failed to send mail to %s.", recipient));
		}
	}
}
