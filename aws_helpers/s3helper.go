package aws_helpers

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/aws/aws-sdk-go/aws"
	session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
	gcp_helpers "github.com/tempuslabs/s3s2/gcp_helpers"
	options "github.com/tempuslabs/s3s2/options"
	utils "github.com/tempuslabs/s3s2/utils"
)

// Given file, open contents and send to S3
func UploadFile(sess *session.Session, org string, aws_key string, local_path string, opts options.Options) error {
	if opts.IsGCS {
		err := retry.Do(
			func() error {
				err := gcp_helpers.UploadFile(org, aws_key, local_path, opts)
				if err != nil {
					log.Infof(("Retrying to upload file %s"), aws_key)
					return err
				} else {
					return nil
				}
			},
			retry.Attempts(5),
		)
		if err != nil {
			utils.PanicIfError("Failed to upload file in multiple attempts exiting execution", err)
			return err
		} else {
			return nil
		}

	} else {

		// Fetch session everytime before uplading to make sure we have latest creds from pod
		sess = utils.GetAwsSession(opts)
		uploader := s3manager.NewUploader(sess)

		file, err := os.Open(local_path)
		utils.PanicIfError("Failed to open file for upload - ", err)

		final_key := utils.ToPosixPath(filepath.Clean(filepath.Join(strings.ToUpper(org), aws_key)))
		log.Debugf("Uploading file '%s' to aws key '%s'", local_path, final_key)

		for {

			if opts.AwsKey != "" {
				result, err := uploader.Upload(&s3manager.UploadInput{
					Bucket:               aws.String(opts.Bucket),
					Key:                  aws.String(final_key),
					ServerSideEncryption: aws.String("aws:kms"),
					SSEKMSKeyId:          aws.String(opts.AwsKey),
					Body:                 file,
				})

				if err != nil {
					utils.PanicIfError("Failed to upload file: ", err)
				} else {
					log.Debugf("File '%s' uploaded to: '%s'", file.Name(), result.Location)
					file.Close()
					return err
				}

			} else {
				// The pod will have empty creds while refreshing the session, in that case we will retry after 10 secs
				err := retry.Do(
					func() error {
						result, err := uploader.Upload(&s3manager.UploadInput{
							Bucket: aws.String(opts.Bucket),
							Key:    aws.String(final_key),
							Body:   file,
						})
						if err != nil {
							time.Sleep(10 * time.Second)
							sess = utils.GetAwsSession(opts)
							uploader = s3manager.NewUploader(sess)
							return err
						}
						log.Debugf("File '%s' uploaded to: '%s'", file.Name(), result.Location)
						file.Close()
						return nil
					},
					retry.Attempts(3),
				)
				utils.PanicIfError("Failed to upload file: ", err)
				return err
			}
		}
	}
}

// Given buffer, send to S3
func UploadBuffer(sess *session.Session, org string, aws_key string, inputBuffer *bytes.Buffer, local_path string, opts options.Options) error {
	if opts.IsGCS {
		err := retry.Do(
			func() error {
				err := gcp_helpers.UploadBuffer(org, aws_key, inputBuffer, local_path, opts)
				if err != nil {
					log.Infof(("Retrying to upload file %s"), aws_key)
					return err
				} else {
					return nil
				}
			},
			retry.Attempts(5),
		)
		if err != nil {
			return err
		} else {
			return nil
		}

	} else {

		// Fetch session everytime before uplading to make sure we have latest creds from pod
		sess = utils.GetAwsSession(opts)
		uploader := s3manager.NewUploader(sess)

		file := inputBuffer

		final_key := utils.ToPosixPath(filepath.Clean(filepath.Join(strings.ToUpper(org), aws_key)))
		log.Debugf("Uploading file '%s' to aws key '%s'", local_path, final_key)

		for {

			if opts.AwsKey != "" {
				result, err := uploader.Upload(&s3manager.UploadInput{
					Bucket:               aws.String(opts.Bucket),
					Key:                  aws.String(final_key),
					ServerSideEncryption: aws.String("aws:kms"),
					SSEKMSKeyId:          aws.String(opts.AwsKey),
					Body:                 file,
				})

				if err != nil {
					utils.PanicIfError("Failed to upload file: ", err)
				} else {
					log.Debugf("File '%s' uploaded to: '%s'", local_path, result.Location)
					return err
				}

			} else {
				// The pod will have empty creds while refreshing the session, in that case we will retry after 10 secs
				err := retry.Do(
					func() error {
						result, err := uploader.Upload(&s3manager.UploadInput{
							Bucket: aws.String(opts.Bucket),
							Key:    aws.String(final_key),
							Body:   file,
						})
						if err != nil {
							time.Sleep(10 * time.Second)
							sess = utils.GetAwsSession(opts)
							uploader = s3manager.NewUploader(sess)
							return err
						}
						log.Debugf("File '%s' uploaded to: '%s'", local_path, result.Location)
						return nil
					},
					retry.Attempts(3),
				)
				utils.PanicIfError("Failed to upload file: ", err)
				return err
			}
		}
	}
}

// Dedicated function for uploading our lambda trigger file - our way of communicating that s3s2 is done
func UploadLambdaTrigger(sess *session.Session, org string, folder string, opts options.Options) error {
	if opts.IsGCS == true {
		return gcp_helpers.UploadLambdaTrigger(org, folder, opts)
	} else {
		// Fetch session everytime before uplading to make sure we have latest creds from pod
		sess = utils.GetAwsSession(opts)
		uploader := s3manager.NewUploader(sess)

		file_name := "._lambda_trigger"

		final_key := utils.ToPosixPath(filepath.Clean(filepath.Join(strings.ToUpper(org), folder, file_name)))
		log.Debugf("Uploading file '%s' to aws key '%s'", file_name, final_key)

		if opts.AwsKey != "" {
			result, err := uploader.Upload(&s3manager.UploadInput{
				Bucket:               aws.String(opts.Bucket),
				Key:                  aws.String(final_key),
				ServerSideEncryption: aws.String("aws:kms"),
				SSEKMSKeyId:          aws.String(opts.AwsKey),
				Body:                 strings.NewReader(""),
			})
			utils.PanicIfError("Failed to upload file: ", err)
			log.Debugf("File '%s' uploaded to: '%s'", file_name, result.Location)
			return err

		} else {
			// The pod will have empty creds while refreshing the session, in that case we will retry after 10 secs
			err := retry.Do(
				func() error {
					result, err := uploader.Upload(&s3manager.UploadInput{
						Bucket: aws.String(opts.Bucket),
						Key:    aws.String(final_key),
						Body:   strings.NewReader(""),
					})
					if err != nil {
						time.Sleep(10 * time.Second)
						sess = utils.GetAwsSession(opts)
						uploader = s3manager.NewUploader(sess)
						return err
					}
					log.Debugf("File '%s' uploaded to: '%s'", file_name, result.Location)
					return nil
				},
				retry.Attempts(3),
			)
			utils.PanicIfError("Failed to upload file: ", err)
			return err
		}
	}
}

// Given an aws key, download file to local machine
func DownloadFile(sess *session.Session, bucket string, org string, aws_key string, target_path string, opts options.Options) (string, error) {
	if opts.IsGCS == true {
		return gcp_helpers.DownloadFile(bucket, org, aws_key, target_path)
	} else {
		file, err := os.Create(target_path)
		utils.PanicIfError("Unable to open file - ", err)

		final_key := filepath.Join(strings.ToUpper(org), aws_key)

		log.Debugf("Downloading from key '%s' to file '%s'", final_key, target_path)

		downloader := s3manager.NewDownloader(sess)

		_, err = downloader.Download(file,
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(final_key),
			})

		if err != nil {
			log.Errorf("Error downloading file '%s'", final_key)
		}

		defer file.Close()

		return file.Name(), err
	}
}
