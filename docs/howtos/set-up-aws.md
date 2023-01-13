<!-- Target audience: engineer familiar with the project, helpful direct tone -->

# How To Set Up AWS for Cumulus

This guide will explain how to configure your AWS cloud to add the S3 buckets, Athena tables,
and more that Cumulus will need. It assumes you are familiar with AWS.

At the end, you'll be given a CloudFormation template that will do all the hard work for you.

## End Goal

By the end of this guide, you'll have multiple S3 buckets to both receive the output of Cumulus ETL
and to store the results of Athena queries.

You'll also have defined a Glue database and crawler to map the output of Cumulus ETL onto Glue
tables, which Athena can then query.

But all of that can be grouped into three different stages:
1. Cumulus ETL (S3 buckets)
2. Glue (tables)
3. Athena (bucket & configuration)

### Cumulus ETL

You're going to create two buckets. One for the de-identified Cumulus ETL output and one for build
artifacts (which holds PHI).
They'll each need similar security policies, but Glue will only look at the output bucket.

You also don't _need_ to store your build artifacts in this S3 bucket.
Once you get to actually running Cumulus ETL, there will be an on-premises option for that.
Though you do need to use an S3 bucket for the de-identified Cumulus ETL output.

These buckets will require encryption and allow access to the user role that is running Cumulus ETL.

### Glue

Glue is an AWS product that creates table schemas based on the files in the Cumulus ETL output
bucket.

You're going to create a database to hold the tables and a crawler that scans the Cumulus ETL
output bucket, creating the tables.

After the first crawler run that creates the tables & schemas,
you'll only need to run it again if new tables get added or schemas change
(maybe your hospital started adding new metadata to FHIR resources).

We'll set this crawler to run once a month, to pick up the occasional schema changes.
But it can also be run manually when you know a change occurred, or a new table was added.

### Athena

Athena is an AWS product that can run SQL queries against Glue tables.
It's how we'll generate the patient counts for studies.

You're going to create a bucket to hold Athena query results and an Athena workgroup to configure
Athena.

## Cloud Formation

The easy way to set this all up is simply use a CloudFormation template.
Here's an example template that should work for your needs, but can be customized as you like.

It takes four parameters:
1. Bucket prefix
1. ETL Subdirectory, matching the subdirectory you pass to Cumulus ETL
1. KMS key ARN for encryption
1. Upload Role ARN, matching the user that runs Cumulus ETL

Once you create this CloudFormation stack, you're almost done.
There is one step left below this template: updating the Glue crawler.

```yaml
AWSTemplateFormatVersion: 2010-09-09
Description: Create an Athena database for the Cumulus project

Parameters:
  BucketPrefix:
    Type: 'String'
    Description: 'Prefix for Cumulus bucket names (they will look like ${BucketPrefix}-${purpose}-${AWS::AccountId}-${AWS::Region})'
  EtlSubdir:
    Type: 'String'
    Description: 'Subdirectory on the Cumulus ETL output bucket where files will be placed. This should match the path you give when running Cumulus ETL. Using a subdirectory is recommended to allow for test runs of Cumulus ETL in different subdirectories and general future-proofing.'
  KMSMasterKeyID:
    Type: 'String'
    Description: 'KMS key ARN for Cumulus buckets'
  UploadRoleArn:
    Type: 'String'
    Description: 'ARN for role that is running Cumulus ETL and thus uploading files to S3'

Resources:
  ####################################################
  # S3 Buckets for raw Cumulus output
  ####################################################

  S3Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub ${BucketPrefix}-${AWS::AccountId}-${AWS::Region}
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault:
              KMSMasterKeyID: !Ref KMSMasterKeyID
              SSEAlgorithm: aws:kms
      PublicAccessBlockConfiguration:
        BlockPublicAcls: True
        BlockPublicPolicy: True
        IgnorePublicAcls: True
        RestrictPublicBuckets: True

  S3BucketPolicy:
    Type: AWS::S3::BucketPolicy
    Properties:
      Bucket: !Ref S3Bucket
      PolicyDocument:
        Version: 2012-10-17
        Statement:
          - Sid: AWSBucketRequiresEncryption
            Effect: Deny
            Principal: "*"
            Action: s3:PutObject
            Resource: !Sub "arn:aws:s3:::${S3Bucket}/*"
            Condition:
              StringNotEquals:
                s3:x-amz-server-side-encryption: aws:kms
          - Sid: AWSBucketAllowUploads
            Effect: Allow
            Principal:
              AWS: !Ref UploadRoleArn
            Action:
              - s3:DeleteObject
              - s3:PutObject
            Resource: !Sub "arn:aws:s3:::${S3Bucket}/*"
          - Sid: AWSBucketAllowListing
            Effect: Allow
            Principal:
              AWS: !Ref UploadRoleArn
            Action:
              - s3:ListBucket
            Resource: !Sub "arn:aws:s3:::${S3Bucket}"

  PHIBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub ${BucketPrefix}-phi-${AWS::AccountId}-${AWS::Region}
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault:
              KMSMasterKeyID: !Ref KMSMasterKeyID
              SSEAlgorithm: aws:kms
      PublicAccessBlockConfiguration:
        BlockPublicAcls: True
        BlockPublicPolicy: True
        IgnorePublicAcls: True
        RestrictPublicBuckets: True

  PHIBucketPolicy:
    Type: AWS::S3::BucketPolicy
    Properties:
      Bucket: !Ref PHIBucket
      PolicyDocument:
        Version: 2012-10-17
        Statement:
          - Sid: AWSBucketRequiresEncryption
            Effect: Deny
            Principal: "*"
            Action: s3:PutObject
            Resource: !Sub "arn:aws:s3:::${PHIBucket}/*"
            Condition:
              StringNotEquals:
                s3:x-amz-server-side-encryption: aws:kms
          - Sid: AWSBucketAllowAccess
            Effect: Allow
            Principal:
              AWS: !Ref UploadRoleArn
            Action:
              - s3:GetObject
              - s3:PutObject
            Resource: !Sub "arn:aws:s3:::${PHIBucket}/*"

  ####################################################
  # Glue database & tables for raw Cumulus data
  ####################################################

  GlueSecurity:
    Type: AWS::Glue::SecurityConfiguration
    Properties:
      EncryptionConfiguration:
        S3Encryptions:
          - KmsKeyArn: !Ref KMSMasterKeyID
            S3EncryptionMode: SSE-KMS
      Name: cumulus-kms

  CrawlerRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: 2012-10-17
        Statement:
          - Effect: Allow
            Principal:
              Service:
                - glue.amazonaws.com
            Action:
              - sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
      Policies:
        - PolicyName: S3BucketAccessPolicy
          PolicyDocument:
            Version: 2012-10-17
            Statement:
              - Effect: Allow
                Action:
                  - s3:GetObject
                  - s3:PutObject
                Resource: !Sub "arn:aws:s3:::${S3Bucket}/*"

  GlueDB:
    Type: AWS::Glue::Database
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseInput:
        Name: cumulus

  GlueCrawler:
    Type: AWS::Glue::Crawler
    Properties:
      Name: cumulus
      DatabaseName: !Ref GlueDB
      Role: !GetAtt CrawlerRole.Arn
      CrawlerSecurityConfiguration: !Ref GlueSecurity
      RecrawlPolicy:
        RecrawlBehavior: CRAWL_EVERYTHING
      SchemaChangePolicy:
        DeleteBehavior: DEPRECATE_IN_DATABASE
        UpdateBehavior: UPDATE_IN_DATABASE
      Schedule:
        # Schedule a monthly run to catch any newly-added fields automatically
        ScheduleExpression: "cron(0 8 1 * ? *)"  # 8am on the 1st of the month
      Targets:
        # This S3Targets definition is suitable for parquet files, but we won't actually be using it.
        # You'll want to use a Delta Lake crawler instead.
        # CloudFormation requires *a* target be defined, but it does not yet support DeltaTargets.
        # So we provide a valid definition, for a target that we don't actually intend to use.
        # We'll later replace these manually with some DeltaTargets.
        S3Targets:
          - Path: !Sub "s3://${S3Bucket}/${EtlSubdir}/"
            Exclusions:
              - "JobConfig/**"

  ####################################################
  # Athena queries and where to store them
  ####################################################

  AthenaBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub ${BucketPrefix}-athena-${AWS::AccountId}-${AWS::Region}
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault:
              KMSMasterKeyID: !Ref KMSMasterKeyID
              SSEAlgorithm: aws:kms
      PublicAccessBlockConfiguration:
        BlockPublicAcls: True
        BlockPublicPolicy: True
        IgnorePublicAcls: True
        RestrictPublicBuckets: True

  AthenaBucketPolicy:
    Type: AWS::S3::BucketPolicy
    Properties:
      Bucket: !Ref AthenaBucket
      PolicyDocument:
        Version: 2012-10-17
        Statement:
          # We don't expect non-Athena uploads to this bucket, but just as a safeguard against misconfiguration,
          # let's enforce encryption on all incoming data.
          - Sid: AWSBucketRequiresEncryption
            Effect: Deny
            Principal: "*"
            Action: s3:PutObject
            Resource: !Sub "arn:aws:s3:::${AthenaBucket}/*"
            Condition:
              StringNotEquals:
                s3:x-amz-server-side-encryption: aws:kms

  AthenaWorkGroup:
    Type: AWS::Athena::WorkGroup
    Properties:
      Name: cumulus
      State: ENABLED
      WorkGroupConfiguration:
        EnforceWorkGroupConfiguration: True
        PublishCloudWatchMetricsEnabled: True
        EngineVersion:
          SelectedEngineVersion: "Athena engine version 2"
        ResultConfiguration:
          EncryptionConfiguration:
            EncryptionOption: SSE_KMS
            KmsKey: !Ref KMSMasterKeyID
          OutputLocation: !Sub "s3://${AthenaBucket}/"

Outputs:
  BucketName:
    Description: Cumulus de-identified output bucket ID
    Value: !Ref S3Bucket
  PhiBucketName:
    Description: Cumulus PHI output bucket ID
    Value: !Ref PHIBucket
  AthenaBucketName:
    Description: Cumulus Athena results bucket ID
    Value: !Ref AthenaBucket
```

## Delta Lake Crawler Support

Cumulus uses Delta Lakes to store your data.
AWS Glue support for them is fairly new (September 2022),
and CloudFormation does not yet support that syntax.

But that's easy enough to work around.
We'll just manually update the crawler to point at our delta lakes.

Run the command below to update the crawler you just defined above,
and replace `REPLACE_ME` with a bucket path (the bucket name and `EtlSubdir` you used above).

For example, you might use `s3://my-cumulus-prefix-99999999999-us-east-2/subdir1`.

(Make sure you have `jq` installed first.)

```sh
aws glue update-crawler --name cumulus --targets "`jq -n --arg prefix REPLACE_ME '{"DeltaTargets": [{"DeltaTables": [$prefix+"/condition", $prefix+"/covid_symptom__nlp_results", $prefix+"/documentreference", $prefix+"/encounter", $prefix+"/observation", $prefix+"/patient"], "WriteManifest": false}]}'`"
```
