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

The buckets will require encryption and grant access to the user role that is running Cumulus ETL.

### Glue

Glue is an AWS product that creates table schemas based on the files in the Cumulus ETL output
bucket.

You're going to create a database to hold the tables and a crawler that scans the Cumulus ETL
output bucket, creating the tables.
This crawler can simply be a manually run job for now.

### Athena

Athena is an AWS product that can run SQL queries against Glue tables.
It's how we'll generate the patient counts for studies.

You're going to create a bucket to hold Athena query results and an Athena workgroup to configure
Athena.

## Cloud Formation

The easy way to set this all up is simply use a CloudFormation template.
Here's an example one that should work for your needs.

It takes four parameters:
1. Bucket prefix
1. ETL Subdirectory, matching the subdirectory you pass to Cumulus ETL
1. KMS key ID for encryption
1. Upload Role ARN, matching the user that runs Cumulus ETL

Once you create this CloudFormation stack, your infrastructure will be ready to run Cumulus ETL.

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
    Description: 'KMS key ID for Cumulus buckets'
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
          - KmsKeyArn: !Sub "arn:aws:kms:${AWS::Region}:${AWS::AccountId}:key/${KMSMasterKeyID}"
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
      Targets:
        S3Targets:
          - Path: !Sub "s3://${S3Bucket}/${EtlSubdir}/"
            Exclusions:
              - "JobConfig/**"
            SampleSize: 1

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
