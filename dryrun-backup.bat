aws --profile backup s3 sync %~dp0 s3://dkwasny-personal-backup/ --storage-class DEEP_ARCHIVE --dryrun
