echo "Running S3S2 tests..."
apt update && apt install ca-certificates libgnutls30 -y
go test -v ./tests
