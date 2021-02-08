# plawsy

- play in AWS

### class and function

- TakeAccessKey
    - take your aws secret key (will be update using AWS Secret Manager)
- S3Connector
    - S3 file IO
    - get_bucket_list() : you can get your bucket list
    - downlaod(bucket_nm, remote_fn) : you can download your file from AWS S3 (json, csv, xlsx, pkl, txt etc.)
    - uplaod(bucket_nm, remote_fn, upload) : you can upload your file to AWS S3 (json, csv, xlsx, pkl, txt etc.)
    