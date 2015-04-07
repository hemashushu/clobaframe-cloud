Clobaframe-Amazon
=================

The clobaframe-blobstore and clobaframe-mail implementations for Amazon Web Service.

Unit test
---------

1. Complete all clobaframe unit tests.
2. Install clobaframe library into local Apache Maven repository.
3. Check out the clobaframe-amazon source code to any folder.
4. Register AWS and get a credentials file `AwsCredentials.properties`, and save the file into folder `/etc/clobaframe`. The content of the credential should looks like this:

```
    secretKey=......
    accessKey=......
```

Make sure the current operation system login user has the file read permission, this can be done with the command:

```bash
$ sudo chmod o+r /etc/clobaframe/AwsCredentials.properties
```

5. Run unit test with Apache Maven:

```bash
$ mvn clean test
```

Install library
---------------

Install clobaframe-amazon into Apache Maven local repository:

```bash
$ mvn clean javadoc:jar source:jar install
```
