package gcp_helper

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	log "github.com/sirupsen/logrus"
	options "github.com/tempuslabs/s3s2/options"
	utils "github.com/tempuslabs/s3s2/utils"
	"google.golang.org/api/option"
)

// Given file, open contents and send to S3
func UploadFile(org string, aws_key string, local_path string, opts options.Options) error {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	defer client.Close()

	utils.PanicIfError("Unable to get clients - ", err)

	file, err := os.Open(local_path)
	defer file.Close()

	utils.PanicIfError("Failed to open file for upload - ", err)

	final_key := utils.ToPosixPath(filepath.Clean(filepath.Join(strings.ToUpper(org), aws_key)))
	log.Debugf("Uploading file '%s' to aws key '%s'", local_path, final_key)

	o := client.Bucket(opts.Bucket).Object(final_key)
	if strings.Contains(final_key, "s3s2_manifest.json") {
		log.Infof("Uploading manifest'", final_key)
	}

	wc := o.NewWriter(ctx)
	wc.ContentType = "text/plain"

	_, err = io.Copy(wc, file)

	if err != nil {
		log.Errorf("Failed to upload file while writing: %s", final_key)
		return err
	} else {
		log.Debugf("File '%s' uploaded to:  bucket = '%s', key = '%s'", file.Name(), opts.Bucket, final_key)
	}

	err = wc.Close()

	if err != nil {
		log.Errorf("Failed to upload file while closing writer: %s", final_key)
		return err
	} else {
		log.Debugf("File '%s' uploaded to:  bucket = '%s', key = '%s'", file.Name(), opts.Bucket, final_key)
	}

	return nil

}

// Given buffer, send to GCS
func UploadBuffer(org string, aws_key string, inputBuffer *bytes.Buffer, local_path string, opts options.Options) error {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	utils.PanicIfError("Unable to get clients - ", err)
	defer client.Close()

	utils.PanicIfError("Failed to open file for upload - ", err)

	final_key := utils.ToPosixPath(filepath.Clean(filepath.Join(strings.ToUpper(org), aws_key)))
	log.Debugf("Uploading file '%s' to aws key '%s'", local_path, final_key)

	wc := client.Bucket(opts.Bucket).Object(final_key).NewWriter(ctx)

	wc.ContentType = "text/plain"

	_, err = io.Copy(wc, inputBuffer)

	if err != nil {
		log.Errorf("Failed to upload file while writing: %s", final_key)
		return err

	} else {
		log.Debugf("File '%s' uploaded to:  bucket = '%s', key = '%s'", local_path, opts.Bucket, final_key)
	}

	err = wc.Close()

	if err != nil {
		log.Errorf("Failed to upload file while closing writer: %s", final_key)
		return err
	} else {
		log.Debugf("File '%s' uploaded to:  bucket = '%s', key = '%s'", local_path, opts.Bucket, final_key)
	}

	return nil

}

// Dedicated function for uploading our lambda trigger file - our way of communicating that s3s2 is done
func UploadLambdaTrigger(org string, folder string, opts options.Options) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	defer client.Close()
	utils.PanicIfError("Unable to get clients - ", err)

	file_name := "._lambda_trigger"
	file, err := os.Create(file_name)
	defer file.Close()
	bucket := opts.Bucket
	final_key := utils.ToPosixPath(filepath.Clean(filepath.Join(strings.ToUpper(org), folder, file_name)))
	log.Debugf("Uploading file '%s' to bucket '%s' aws key '%s'", file_name, bucket, final_key)
	wc := client.Bucket(bucket).Object(final_key).NewWriter(ctx)
	defer wc.Close()
	_, err = io.Copy(wc, file)

	utils.PanicIfError("Failed to upload file: ", err)
	log.Debugf("File '%s' uploaded to", file_name)
	return err
}

// Given an aws key, download file to local machine
func DownloadFile(bucket string, org string, aws_key string, target_path string) (string, error) {

	file, err := os.Create(target_path)
	utils.PanicIfError("Unable to open file - ", err)
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")))
	utils.PanicIfError("Unable to get context client - ", err)

	final_key := filepath.Join(strings.ToUpper(org), aws_key)

	rc, err := client.Bucket(bucket).Object(final_key).NewReader(ctx)
	utils.PanicIfError("Unable to get object - ", err)

	log.Infof("Downloading from key '%s' to file '%s'", final_key, target_path)

	data, err := ioutil.ReadAll(rc)
	utils.PanicIfError("Unable to download file at ioutil.ReadAll - ", err)
	defer rc.Close()

	_, err = file.Write(data)
	utils.PanicIfError("Error downloading file to local a file - ", err)

	defer file.Close()

	return file.Name(), err
}

// Given bucket and key check if file exists
func CheckFileExists(bucket string, org string, aws_key string) (string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	utils.PanicIfError("Unable to get context client - ", err)

	final_key := filepath.Join(strings.ToUpper(org), aws_key)

	rc, err := client.Bucket(bucket).Object(final_key).NewReader(ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()

	return final_key, nil
}
