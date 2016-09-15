# Informations how to use S3 for uploading JAR files to use in Spark submit

The [Amazon Simple Storage Service (S3)](https://aws.amazon.com/s3/getting-started/) can be used 
to exchange files between your local development machine and your cluster.

However, since your AWS credentials will not be known to all things running on the cluster, data will need to be public and the files you upload may not contain any confidential information 
- or you should better look for an alternative option. Unfortunately, in [DC/OS 1.7 using SSH with rsync does not longer work](https://github.com/Zuehlke/SHMACK/issues/16) nicely and it is yet not clear, 
whether the new [DC/OS Tunnel-CLI](https://github.com/mesosphere/universe/commit/f70411e71a0928dce80084667ea9a02c8d975c8e) in DC/OS 1.8 will bring back this possibility.   
    
It can be used from the [Amazon AWS Web Concole](https://console.aws.amazon.com/s3/home?region=us-west-1#), but for repeated use, the commandline is far more convenient.

# Creating a (publicly readable) bucket

*Buckets* are the root level in which S3 stores the information - and they should intensionally not be assigned to a single account, so they need to have a unique name of their own.

You can make a new bucket with `aws s3 mb s3://<bucketname>`

Keep in mind that buckets are assigned a region, and this command will make the bucket in your default region - which should be `us-west-1`, which is fine.
 
Now make this bucket readable by everyone:
`aws s3api put-bucket-acl --bucket shmack23 --grant-read 'uri="http://acs.amazonaws.com/groups/global/AllUsers"'`

(See <http://docs.aws.amazon.com/cli/latest/userguide/using-s3api-commands.html> for details.)

# Uploading a (jar) file 

Upload the file from commandline directly making it readable to everyone:
`aws s3 cp build/libs/<your.jar> s3://<bucketname> --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers`

# Accessing the file

The uploaded file is now accessible via a URL like `https://s3-us-west-1.amazonaws.com/<bucketname>/<your.jar>`

This URL can get used in `dcos spark run --submit-args=" https://s3-us-west-1.amazonaws.com/<bucketname>/<your.jar>"`