echo "Running S3S2 tests..."
apt -qq update && apt -qq install ca-certificates libgnutls30 -y
go test -v ./tests
