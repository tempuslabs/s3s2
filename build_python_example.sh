CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 go build -buildmode=c-shared -o so/s3s2.so sharedobj/sodecrypt.go
python python_example.py