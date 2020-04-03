
package cmd

import (
	"sync"
	"time"
	"fmt"
	"strings"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
    "golang.org/x/crypto/openpgp/packet"

    session "github.com/aws/aws-sdk-go/aws/session"

    log "github.com/sirupsen/logrus"

    // local
	zip "github.com/tempuslabs/s3s2/zip"
	encrypt "github.com/tempuslabs/s3s2/encrypt"
	manifest "github.com/tempuslabs/s3s2/manifest"
	options "github.com/tempuslabs/s3s2/options"
	aws_helpers "github.com/tempuslabs/s3s2/aws_helpers"
	file "github.com/tempuslabs/s3s2/file"
	utils "github.com/tempuslabs/s3s2/utils"

)

// shareCmd represents the share command
var shareCmd = &cobra.Command{
	Use:   "share",
	Short: "Encrypt and upload to S3",
	Long: `Given a directory, encrypt all non-private-file contents and upload to S3.
    Behind the scenes, S3S2 checks to ensure the file is
    either GPG encrypted or passes S3 headers indicating
    that it will be encrypted.`,
	// bug in Viper prevents shared flag names across different commands
	// placing these in the prerun is the workaround
	PreRun: func(cmd *cobra.Command, args []string) {
		viper.BindPFlag("directory", cmd.Flags().Lookup("directory"))
		viper.BindPFlag("region", cmd.Flags().Lookup("region"))
		viper.BindPFlag("parallelism", cmd.Flags().Lookup("parallelism"))
		cmd.MarkFlagRequired("directory")
		cmd.MarkFlagRequired("org")
		cmd.MarkFlagRequired("region")
	},
	Run: func(cmd *cobra.Command, args []string) {

		opts := buildShareOptions(cmd)
		checkShareOptions(opts)

		start := time.Now()
		fnuuid := start.Format("20060102150405") // golang uses numeric constants for timestamp formatting
		batch_folder := opts.Prefix + "_s3s2_" + fnuuid

		file_structs, file_structs_metadata, err := file.GetFileStructsFromDir(opts.Directory, opts)
		utils.PanicIfError("Error reading directory", err)

		if len(file_structs) == 0 && len(file_structs_metadata) == 0 {
		    panic("No files from input directory were read. This means the directory is empty or only contains invalid files.")
		}

		file_struct_batches := append([][]file.File{file_structs_metadata}, file.ChunkArray(file_structs, opts.BatchSize)...)

	    var work_folder string
        if opts.ScratchDirectory != "" {
            work_folder = opts.ScratchDirectory
        } else {
            work_folder = opts.Directory
        }

        sess := utils.GetAwsSession(opts)
	    _pubKey := encrypt.GetPubKey(sess, opts)

	    sem := make(chan int, opts.Parallelism)

		change_s3_folders_at_size := 100000
        current_s3_folder_size := 0

		var all_uploaded_files_so_far []file.File
		var m manifest.Manifest
		var wg sync.WaitGroup

        // for each batch
		for i_batch, batch := range file_struct_batches {
		    wg.Add(len(batch))

		    log.Infof("Processing batch '%d'...", i_batch)

		    // refresh session every batch
		    sess = utils.GetAwsSession(opts)

            // tie off this current s3 directory
		    if current_s3_folder_size + len(batch) > change_s3_folders_at_size {
		    	aws_helpers.UploadLambdaTrigger(sess, opts.Org, batch_folder, opts)
		        current_s3_folder_size = 0
		        all_uploaded_files_so_far = []file.File{}
		    }

            // for each file in batch
            for _, fs := range batch {
                go func(wg *sync.WaitGroup, sess *session.Session, _pubkey *packet.PublicKey, folder string, fs file.File, opts options.Options) {
                    sem <- 1
                    defer func() { <-sem }()
                    defer wg.Done()
                    err = processFile(sess, _pubKey, batch_folder, work_folder, fs, opts)
                }(&wg, sess, _pubKey, batch_folder, fs, opts)
            }
            wg.Wait()

            current_s3_folder_size += len(batch)
		    all_uploaded_files_so_far = append(all_uploaded_files_so_far, batch...)

            // upon batch completion
            m, err = manifest.BuildManifest(all_uploaded_files_so_far, batch_folder, opts)
            utils.PanicIfError("Error building Manifest", err)

            // create manifest in top-level directory - overwrite any existing manifest to include latest batch
            manifest_aws_key := filepath.Join(batch_folder, m.Name)
            manifest_local := filepath.Join(opts.Directory, m.Name)
            err = aws_helpers.UploadFile(sess, opts.Org, manifest_aws_key, manifest_local, opts)
            utils.PanicIfError("Error uploading Manifest", err)

            // archive the files we processed in this batch, dont archive metadata files until entire process is done
            if opts.ArchiveDirectory != "" && i_batch != 0 {
                log.Info("Archiving files in batch '%d'", i_batch)
                file.ArchiveFileStructs(batch, opts.Directory, opts.ArchiveDirectory)
            }

            log.Infof("Successfully processed batch '%d'", i_batch)

        }
        // archive metafiles now
        if opts.ArchiveDirectory != "" {
            file.ArchiveFileStructs(file_structs_metadata, opts.Directory, opts.ArchiveDirectory)
        }

		utils.Timing(start, "Elapsed time: %f")
		aws_helpers.UploadLambdaTrigger(sess, opts.Org, batch_folder, opts)
	},
}

func processFile(sess *session.Session, _pubkey *packet.PublicKey, aws_folder string, work_folder string, fs file.File, opts options.Options) error {
	log.Debugf("Processing file '%s'", fs.Name)
	start := time.Now()

	fn_source := fs.GetSourceName(opts.Directory)
	fn_zip := fs.GetZipName(work_folder)
	fn_encrypt := fs.GetEncryptedName(work_folder)
	fn_aws_key := fs.GetEncryptedName(aws_folder)

	zip.ZipFile(fn_source, fn_zip, work_folder)
	encrypt.EncryptFile(_pubkey, fn_zip, fn_encrypt, opts)

	err := aws_helpers.UploadFile(sess, opts.Org, fn_aws_key, fn_encrypt, opts)

	if err != nil {
	    log.Error("Error uploading file - ", err)
	} else {
	    utils.Timing(start, fmt.Sprintf("\tProcessed file '%s' in ", fs.Name) + "%f seconds")
	}

	// cleanup regardless of the upload succeeding or not, we will retry outside of this function
    utils.CleanupFile(fn_zip)
	utils.CleanupFile(fn_encrypt)

    return err
}


// buildContext sets up the ShareContext we're going to use
// to keep track of our state while we go.
func buildShareOptions(cmd *cobra.Command) options.Options {

	directory := filepath.Clean(viper.GetString("directory"))
	awsKey := viper.GetString("awskey")
	bucket := viper.GetString("bucket")
	region := viper.GetString("region")
	org := viper.GetString("org")
	prefix := viper.GetString("prefix")
	pubKey := filepath.Clean(viper.GetString("receiver-public-key"))
	archive_directory := viper.GetString("archive-directory")
	scratch_directory := viper.GetString("scratch-directory")
	aws_profile := viper.GetString("aws-profile")
	parallelism := viper.GetInt("parallelism")
	batchSize := viper.GetInt("batch-size")
	metaDataFiles := strings.Split(viper.GetString("metadata-files"), ",")

	options := options.Options{
		Directory       : directory,
		AwsKey          : awsKey,
		Bucket          : bucket,
		Region          : region,
		Org             : org,
		Prefix          : prefix,
		PubKey          : pubKey,
		ScratchDirectory: scratch_directory,
		ArchiveDirectory: archive_directory,
		AwsProfile      : aws_profile,
		Parallelism     : parallelism,
		BatchSize       : batchSize,
		MetaDataFiles   : metaDataFiles,
	}

	debug := viper.GetBool("debug")
	if debug != true {
		log.SetLevel(log.InfoLevel)
	}
	log.Debug("Captured options: ")
	log.Debug(options)

	return options
}

func checkShareOptions(options options.Options) {
	if options.AwsKey == "" && options.PubKey == "" {
		log.Warn("Need to supply either AWS Key for S3 level encryption or a public key for GPG encryption or both!")
		log.Panic("Insufficient key material to perform safe encryption.")
	} else if options.Bucket == "" {
		log.Warn("Need to supply a bucket.")
		log.Panic("Insufficient information to perform decryption.")
	} else if options.Directory == "" {
		log.Warn("Need to supply a destination for the files to decrypt.  Should be a local path.")
		log.Panic("Insufficient information to perform decryption.")
	}

	if !strings.Contains(strings.ToLower(options.Prefix), "clinical") && !strings.Contains(strings.ToLower(options.Prefix), "documents") {
	    log.Errorf("Input Prefix argument is '%s'", options.Prefix)
	    panic("Prefix command line argument must contain 'clinical' or 'documents' to abide by our lambda trigger!")
	}
}

func init() {
	rootCmd.AddCommand(shareCmd)

	shareCmd.PersistentFlags().String("directory", "", "The directory to zip, encrypt and share.")
	shareCmd.MarkFlagRequired("directory")
	shareCmd.PersistentFlags().String("org", "", "The Org that owns the files.")
	shareCmd.MarkFlagRequired("org")
	shareCmd.PersistentFlags().String("prefix", "", "A prefix for the S3 path. Currently used to separate clinical and documents files.")

	shareCmd.PersistentFlags().Int("parallelism", 10, "The maximum number of files to download and decrypt at a time within a batch.")
	shareCmd.PersistentFlags().Int("batch-size", 10000, "Files are uploaded and archived in batches of this size. Manifest files are updated and uploaded after each factor of batch-size.")

    shareCmd.PersistentFlags().String("scratch-directory", "", "If provided, serves as location where .zip & .gpg files are written to. Intended to be leveraged if location will have superior write/read performance. If not provided .zip and .gpg files are written to the original directory.")
    shareCmd.PersistentFlags().String("archive-directory", "", "If provided, contents of upload directory are moved here after each batch.")
    shareCmd.PersistentFlags().String("metadata-files", "", "If provided, these files are the first to be uploaded and the last to be archived out of the input directory. Comma-separated. I.E. --metadata-files=file1,file2,file3")

	shareCmd.PersistentFlags().String("awskey", "", "The agreed upon S3 key to encrypt data with at the bucket.")
	shareCmd.PersistentFlags().String("receiver-public-key", "", "The receiver's public key.  A local file path.")
	shareCmd.PersistentFlags().String("aws-profile", "", "AWS Profile to use for the session.")

	viper.BindPFlag("directory", shareCmd.PersistentFlags().Lookup("directory"))
	viper.BindPFlag("org", shareCmd.PersistentFlags().Lookup("org"))
	viper.BindPFlag("prefix", shareCmd.PersistentFlags().Lookup("prefix"))
	viper.BindPFlag("parallelism", shareCmd.PersistentFlags().Lookup("parallelism"))
	viper.BindPFlag("batch-size", shareCmd.PersistentFlags().Lookup("batch-size"))
	viper.BindPFlag("scratch-directory", shareCmd.PersistentFlags().Lookup("scratch-directory"))
	viper.BindPFlag("archive-directory", shareCmd.PersistentFlags().Lookup("archive-directory"))
	viper.BindPFlag("metadata-files", shareCmd.PersistentFlags().Lookup("metadata-files"))
	viper.BindPFlag("awskey", shareCmd.PersistentFlags().Lookup("awskey"))
	viper.BindPFlag("receiver-public-key", shareCmd.PersistentFlags().Lookup("receiver-public-key"))
	viper.BindPFlag("aws-profile", shareCmd.PersistentFlags().Lookup("aws-profile"))

	//log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{})
	log.SetLevel(log.DebugLevel)
}
