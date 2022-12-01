go build
GOOS=darwin GOARCH=amd64 go build -buildmode=c-shared -o so/s3s2.so sharedobj/sodecrypt.go
python python_example.py