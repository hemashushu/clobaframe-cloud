clobaframe-amazon
=================

The clobaframe implements for Amazon Web Service.

Unit test
---------

1. Complete all clobaframe unit tests first.

2. Check out source code to any folder.

3. Register AWS and get a credentials file, and save the file into /etc/clobaframe folder.

/etc/clobaframe/AwsCredentials.properties

The content of the credential should looks like this:

secretKey=......
accessKey=......

Make sure the current login user has the file read permission, this can be done with the command:

$ sudo chmod o+r /etc/clobaframe/AwsCredentials.properties

4. Run unit test with Apache Maven:
    $mvn clean test

Install library
---------------

Install clobaframe into Apache Maven local repository:

    $ mvn clean javadoc:jar source:jar install

