GOOS=darwin GOARCH=amd64 go build -o darwin/s3s2-darwin-amd64 -v

./darwin/s3s2-darwin-amd64  decrypt --directory ~/Desktop/aws/dec/ --region us-west-2  --file someorg/somes3s2batch/s3s2_manifest.json --bucket somebucket --is-gcs true --ssm-public-key /pathtoparam/PUBLIC_KEY_S3S2 --ssm-private-key /pathtoparam/PRIVATE_KEY_S3S2 --filter-files */SalesDB2014_Split2* --aws-profile someprofile