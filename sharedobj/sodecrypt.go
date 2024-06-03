package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/openpgp/packet"

	session "github.com/aws/aws-sdk-go/aws/session"

	log "github.com/sirupsen/logrus"

	// local
	aws_helpers "github.com/tempuslabs/s3s2/aws_helpers"
	encrypt "github.com/tempuslabs/s3s2/encrypt"
	file "github.com/tempuslabs/s3s2/file"
	manifest "github.com/tempuslabs/s3s2/manifest"
	options "github.com/tempuslabs/s3s2/options"
	utils "github.com/tempuslabs/s3s2/utils"
	wc_helpers "github.com/tempuslabs/s3s2/wc_helpers"
	zip "github.com/tempuslabs/s3s2/zip"
)
import "C"

var opts options.Options

//export Decrypt
func Decrypt(
	bucket string,
	f string,
	directory string,
	org string,
	region string,
	awsProfile string,
	privKey string,
	pubKey string,
	ssmPrivKey string,
	ssmPubKey string,
	isGCS bool,
	parallelism int,
	filterFiles string) int {

	opts := options.Options{
		Bucket:      bucket,
		File:        f,
		Directory:   directory,
		Org:         org,
		Region:      region,
		PrivKey:     privKey,
		PubKey:      pubKey,
		IsGCS:       isGCS,
		SSMPrivKey:  ssmPrivKey,
		SSMPubKey:   ssmPubKey,
		AwsProfile:  awsProfile,
		Parallelism: parallelism,
		FilterFiles: filterFiles,
	}
	checkDecryptOptions(opts)

	// top level clients
	sess := utils.GetAwsSession(opts)
	_pubKey := encrypt.GetPubKey(sess, opts)
	_privKey := encrypt.GetPrivKey(sess, opts)

	os.MkdirAll(opts.Directory, os.ModePerm)

	// if downloading via manifest
	if strings.HasSuffix(opts.File, "manifest.json") {

		log.Info("Detected manifest file...")

		target_manifest_path := filepath.Join(opts.Directory, filepath.Base(opts.File))
		fn, err := aws_helpers.DownloadFile(sess, opts.Bucket, opts.Org, opts.File, target_manifest_path, opts)
		utils.PanicIfError("Unable to download file at strings.HasSuffix - ", err)

		m := manifest.ReadManifest(fn)
		batch_folder := m.Folder
		file_structs := m.Files

		if len(opts.FilterFiles) >= 1 {
			patterns := strings.Split(opts.FilterFiles, ",")
			var file_filtered []file.File

			for _, f_pattern := range patterns {
				for _, fs := range file_structs {
					reg_pattern := wc_helpers.WildCardToRegex(f_pattern)
					r, _ := regexp.Compile(reg_pattern)
					if r.MatchString(fs.Name) {
						log.Infof("Matched %v with %s", fs, reg_pattern)
						file_filtered = append(file_filtered, fs)
					}
				}

			}
			file_structs = file_filtered
		}

		var wg sync.WaitGroup
		sem := make(chan int, opts.Parallelism)
		for _, fs := range file_structs {
			wg.Add(1)
			go func(wg *sync.WaitGroup, sess *session.Session, _pubkey *packet.PublicKey, _privKey *packet.PrivateKey, folder string, fs file.File, opts options.Options) {
				sem <- 1
				defer func() { <-sem }()
				defer wg.Done()
				// if block is for cases where AWS session expires, so we re-create session and attempt file again
				err, skipped := decryptFile(sess, _pubKey, _privKey, m, fs, opts)
				if err != nil || skipped {
					sess = utils.GetAwsSession(opts)
					err, skipped := decryptFile(sess, _pubKey, _privKey, m, fs, opts)
					if err != nil {
						log.Warn("Error during decrypt-file session expiration if block!")
						log.Errorf("Error: '%v'", err)
						panic(err)
					}
					if skipped {
						f, err := os.OpenFile("skipped.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
						if err != nil {
							log.Errorf("Error opening skipped.txt: %v", err)
						} else {
							if _, err := f.WriteString(fs.Name + "\n"); err != nil {
								log.Errorf("Error writing to skipped.txt: %v", err)
							}
							f.Close()
						}
					}
				}
				if skipped {
					f, err := os.OpenFile("skipped.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						log.Errorf("Error opening skipped.txt: %v", err)
					} else {
						if _, err := f.WriteString(fs.Name + "\n"); err != nil {
							log.Errorf("Error writing to skipped.txt: %v", err)
						}
						f.Close()
					}
				}
			}(&wg, sess, _pubKey, _privKey, batch_folder, fs, opts)
		}
		wg.Wait()
	}
	return 1
}

func decryptFile(sess *session.Session, _pubkey *packet.PublicKey, _privkey *packet.PrivateKey, m manifest.Manifest, fs file.File, opts options.Options) (error, bool) {
	start := time.Now()
	skipped := false
	log.Debugf("Starting decryption on file '%s'", fs.Name)

	// enforce posix path
	fs.Name = utils.ToPosixPath(fs.Name)

	aws_key := fs.GetEncryptedName(m.Folder)
	target_path := fs.GetEncryptedName(opts.Directory)

	fn_zip := fs.GetZipName(opts.Directory)
	fn_decrypt := fs.GetSourceName("decrypted")

	nested_dir := filepath.Dir(target_path)
	os.MkdirAll(nested_dir, os.ModePerm)

	_, err := aws_helpers.DownloadFile(sess, opts.Bucket, m.Organization, aws_key, target_path, opts)
	utils.PanicIfError("Main download failed - ", err)

	// Check if downloaded file is empty
	fileInfo, err := os.Stat(target_path)
	if err != nil {
		log.Errorf("Error getting file info: %v", err)
		return err, skipped
	}
	if fileInfo.Size() == 0 {
		log.Warningf("Downloaded file '%s' is empty", target_path)
		skipped = true
	} else {
		encrypt.DecryptFile(_pubkey, _privkey, target_path, fn_zip, opts)
		zip.UnZipFile(fn_zip, fn_decrypt, opts.Directory)

		utils.Timing(start, fmt.Sprintf("\tProcessed file '%s' in ", fs.Name)+"%f seconds")
	}

	return err, skipped
}

func checkDecryptOptions(options options.Options) {
	if options.File == "" {
		log.Warn("Need to supply a file to decrypt. Should be the file path within the bucket but not including the bucket.")
		log.Panic("Insufficient information to perform decryption.")
	} else if options.Bucket == "" {
		log.Warn("Need to supply a bucket.")
		log.Panic("Insufficient information to perform decryption.")
	} else if options.Directory == "" {
		log.Warn("Need to supply a destination for the files to decrypt.  Should be a local path.")
		log.Panic("Insufficient information to perform decryption.")
	} else if options.Region == "" {
		log.Warn("Need to supply a region for the S3 bucket.")
		log.Panic("Insufficient information to perform decryption.")
	} else if options.PubKey == "" && options.SSMPubKey == "" {
		log.Warn("Need to supply a public encryption key parameter.")
		log.Panic("Insufficient information to perform decryption.")
	} else if options.PrivKey == "" && options.SSMPrivKey == "" {
		log.Warn("Need to supply a private encryption key parameter.")
		log.Panic("Insufficient information to perform decryption.")
	}
}

func main() {

}
