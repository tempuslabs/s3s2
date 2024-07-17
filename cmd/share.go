package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	zip "github.com/tempuslabs/s3s2/zip"

	federated_identity "github.com/tempuslabs/s3s2/federated_identity"
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
		viper.BindPFlag("aws-profile", cmd.Flags().Lookup("aws-profile"))
		viper.BindPFlag("ssm-public-key", cmd.Flags().Lookup("ssm-public-key"))
		viper.BindPFlag("is-gcs", cmd.Flags().Lookup("is-gcs"))
		viper.BindPFlag("share-from-list", cmd.Flags().Lookup("share-from-list"))
		viper.BindPFlag("aws-role-arn", cmd.Flags().Lookup("aws-role-arn"))
		cmd.MarkFlagRequired("org")
		cmd.MarkFlagRequired("region")
	},
	Run: func(cmd *cobra.Command, args []string) {

		opts := buildShareOptions(cmd)
		checkShareOptions(opts)

        start := time.Now()
        fnuuid := start.Format("20060102150405") // golang uses numeric constants for timestamp formatting
        date_folder := start.Format("20060102")  // required for sharing from list

        var file_structs []file.File
        var file_structs_metadata []file.File
        var err error
        if opts.Directory != "" {
            file_structs, file_structs_metadata, err = file.GetFileStructsFromDir(opts.Directory, opts)
            utils.PanicIfError("Error reading directory", err)
        } else if opts.ShareFromList != "" {
            file_structs, file_structs_metadata, err = file.GetFileStructsFromCsv(opts.ShareFromList, date_folder, opts)
            utils.PanicIfError("Error reading directory", err)
        } else {
            panic("No parameters for sharing directory / index are defined.")
        }

		if len(file_structs) == 0 && len(file_structs_metadata) == 0 {
		    panic("No files from input directory were read. This means the directory is empty or only contains invalid files.")
		}

		file_struct_chunks := append([][]file.File{file_structs_metadata}, file.ChunkArray(file_structs, opts.ChunkSize)...)

	    var work_folder string
        if opts.ScratchDirectory != "" {
            work_folder = filepath.Join(opts.ScratchDirectory, opts.Org)
        } else {
            work_folder = opts.Directory
        }

        sess := utils.GetAwsSession(opts)
	    _pubKey := encrypt.GetPubKey(sess, opts)

	    sem := make(chan int, opts.Parallelism)

		change_s3_folders_at_size := opts.BatchSize + len(file_structs_metadata)
        current_s3_folder_size := 0
        current_s3_batch := 0

        var batch_folder string
		var all_uploaded_files_so_far []file.File
		var m manifest.Manifest
		var wg sync.WaitGroup

		batch_folder = fmt.Sprintf("%s_s3s2_%s_%d", opts.Prefix, fnuuid, current_s3_batch)

        // for each chunk
		for i_chunk, chunk := range file_struct_chunks {

		    log.Debugf("Processing chunk '%d'...", i_chunk)

			// Check if a previous goroutine is running to refresh the session for
			// federated identity. If so, stop it before starting a new one.
			federated_identity.StopRefreshSession()

		    // refresh session every chunk
		    sess = utils.GetAwsSession(opts)
			if opts.AwsRoleArn != "" {
				// Start a new goroutine to refresh the session
				federated_identity.StartRefreshSession(sess, &opts.AwsRoleArn)
			}

            // tie off this current s3 directory allowing us to decrypt in batches of this size
            // this is used to create digestable folders for decrypt
		    if current_s3_folder_size + len(chunk) > change_s3_folders_at_size {

                // fire lambda for the batch we are tieing off
		        if opts.LambdaTrigger == true {
		            aws_helpers.UploadLambdaTrigger(sess, opts.Org, batch_folder, opts)
		        }

                // reset / increment variables
		        current_s3_folder_size = 0
		        current_s3_batch += 1
		        batch_folder = fmt.Sprintf("%s_s3s2_%s_%d", opts.Prefix, fnuuid, current_s3_batch)

                // ensure the new s3 folder also has the metadata files
                for _, mdf := range file_structs_metadata {
                    if opts.Directory != "" {
                        processFile(sess, _pubKey, batch_folder, work_folder, mdf, opts)
                    } else {
                        processFileInMemory(sess, _pubKey, batch_folder, work_folder, mdf, date_folder, opts)
                    }
                    current_s3_folder_size += 1
                }

		        all_uploaded_files_so_far = file_structs_metadata

            }

            wg.Add(len(chunk))

            // for each file in chunk
            for _, fs := range chunk {
                go func(wg *sync.WaitGroup, sess *session.Session, _pubkey *packet.PublicKey, folder string, fs file.File, opts options.Options) {
                    sem <- 1
                    defer func() { <-sem }()
                    defer wg.Done()
                    if opts.Directory != "" {
                        processFile(sess, _pubKey, batch_folder, work_folder, fs, opts)
                    } else {
                        processFileInMemory(sess, _pubKey, batch_folder, work_folder, fs, date_folder, opts)
                    }
                }(&wg, sess, _pubKey, batch_folder, fs, opts)
            }

            wg.Wait()

            current_s3_folder_size += len(chunk)
		    all_uploaded_files_so_far = append(all_uploaded_files_so_far, chunk...)

            var all_uploaded_files_in_batch []file.File
            // If sharing from list, the filename will include the local path. This corrects the directory for the manifest
            if opts.Directory == "" {
                for _, fs := range all_uploaded_files_so_far {
                    _, file_name := filepath.Split(fs.Name)
                    all_uploaded_files_in_batch = append(all_uploaded_files_in_batch, file.File{Name: filepath.Join(date_folder, file_name)})
                }
            } else {
                all_uploaded_files_in_batch = all_uploaded_files_so_far
            }
            // upon chunk completion
            m, err = manifest.BuildManifest(all_uploaded_files_in_batch, batch_folder, opts)
            utils.PanicIfError("Error building Manifest", err)

            // create manifest in top-level directory - overwrite any existing manifest to include latest chunk
            manifest_aws_key := filepath.Join(batch_folder, m.Name)
            manifest_local := filepath.Join(opts.Directory, m.Name)
            err = aws_helpers.UploadFile(sess, opts.Org, manifest_aws_key, manifest_local, opts)
            utils.PanicIfError("Error uploading Manifest", err)

            // archive the files we processed in this batch, dont archive metadata files until entire process is done
            if opts.ArchiveDirectory != "" && i_chunk != 0 {
                log.Infof("Archiving files in chunk '%d'", i_chunk)
                file.ArchiveFileStructs(chunk, opts.Directory, opts.ArchiveDirectory)
            }

            log.Debugf("Successfully processed chunk '%d'", i_chunk)

        }
        // archive metafiles now
        if opts.ArchiveDirectory != "" {
            file.ArchiveFileStructs(file_structs_metadata, opts.Directory, opts.ArchiveDirectory)
        }

        if opts.DeleteOnCompletion == true {
            utils.RemoveContents(opts.Directory)
        }

        if opts.ScratchDirectory != "" {
            os.Remove(work_folder)
        }

		utils.Timing(start, "Elapsed time: %f")
        if opts.LambdaTrigger == true {
            aws_helpers.UploadLambdaTrigger(sess, opts.Org, batch_folder, opts)
        }
    },
}

func processFile(sess *session.Session, _pubkey *packet.PublicKey, aws_folder string, work_folder string, fs file.File, opts options.Options) {
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
	    utils.PanicIfError("Error uploading file - ", err)
	} else {
	    utils.Timing(start, fmt.Sprintf("\tProcessed file '%s' in ", fs.Name) + "%f seconds")
	}

	// remove the zipped and encrypted files
    utils.CleanupFile(fn_zip)
	utils.CleanupFile(fn_encrypt)

    // these file names are often /internal_dir/basename
    // this line is a non-performant way for each file to be responsible for cleaning up the directory they were in
	if opts.ScratchDirectory != "" {
        nested_dir_crypt, _ := filepath.Split(fn_encrypt)
        source_dir_empty, _ := utils.IsDirEmpty(nested_dir_crypt)

        if source_dir_empty == true {
            os.Remove(nested_dir_crypt)
        }
    }
}

func processFileInMemory(sess *session.Session, _pubkey *packet.PublicKey, aws_folder string, work_folder string, fs file.File, date_folder string, opts options.Options) {
    log.Debugf("Processing file '%s'", fs.Name)
    start := time.Now()

    fn_source := fs.Name
    _, file_name := filepath.Split(fn_source)
    fn_aws_key := filepath.Join(aws_folder, date_folder, file_name+".zip.gpg")

    fn_zip := zip.ZipFileInMemory(fn_source, date_folder)

    fn_encrypted := encrypt.EncryptBuffer(_pubkey, fn_zip, opts)

    err := aws_helpers.UploadBuffer(sess, opts.Org, fn_aws_key, fn_encrypted, file_name, opts)

    if err != nil {
        utils.PanicIfError("Error uploading file - ", err)
    } else {
        utils.Timing(start, fmt.Sprintf("\tProcessed file '%s' in ", file_name)+"%f seconds")
    }
}

// buildContext sets up the ShareContext we're going to use
// to keep track of our state while we go.
func buildShareOptions(cmd *cobra.Command) options.Options {

    // if the directory isn't defined, don't Clean a blank string into a file path
    directory := viper.GetString("directory")
    if directory != "" {
        directory = filepath.Clean(directory)
    }

    // if the index file isn't defined, don't Clean a blank string into a file path
    shareFromList := viper.GetString("share-from-list")
    if shareFromList != "" {
        filepath.Clean(shareFromList)
    }

	awsKey := viper.GetString("awskey")
	bucket := viper.GetString("bucket")
	region := viper.GetString("region")
	org := viper.GetString("org")
	prefix := viper.GetString("prefix")

	pubKey := filepath.Clean(viper.GetString("receiver-public-key"))
	ssmPubKey := viper.GetString("ssm-public-key")
	isGCS := viper.GetBool("is-gcs")

	archive_directory := viper.GetString("archive-directory")
	scratch_directory := viper.GetString("scratch-directory")
	aws_profile := viper.GetString("aws-profile")
	parallelism := viper.GetInt("parallelism")
	chunkSize := viper.GetInt("chunk-size")
	batchSize := viper.GetInt("batch-size")
	aws_role_arn := viper.GetString("aws-role-arn")

	deleteOnCompletion := viper.GetBool("delete-on-completion")

	var metaDataFiles []string
	if viper.GetString("metadata-files") != "" {
	    metaDataFiles = strings.Split(viper.GetString("metadata-files"), ",")
	    }

	lambdaTrigger := viper.GetBool("lambda-trigger")

	options := options.Options{
		Directory          : directory,
		AwsKey             : awsKey,
		Bucket             : bucket,
		Region             : region,
		Org                : org,
		Prefix             : prefix,
		PubKey             : pubKey,
		SSMPubKey          : ssmPubKey,
		IsGCS          	   : isGCS,
		ScratchDirectory   : scratch_directory,
		ArchiveDirectory   : archive_directory,
		AwsProfile         : aws_profile,
		Parallelism        : parallelism,
		ChunkSize          : chunkSize,
		BatchSize          : batchSize,
		MetaDataFiles      : metaDataFiles,
		LambdaTrigger      : lambdaTrigger,
		DeleteOnCompletion : deleteOnCompletion,
		ShareFromList      : shareFromList,
		AwsRoleArn		   : aws_role_arn,
	}

	debug := viper.GetBool("debug")
	if debug != true {
		log.SetLevel(log.InfoLevel)
	}

	log.Debugf("Captured options: %s", options)

	return options
}
// Any assertions that need to be made regarding input arguments
func checkShareOptions(options options.Options) {
    log.Debug("Checking input arguments...")

	if options.AwsKey == "" && options.PubKey == "" && options.SSMPubKey == "" {
		panic("Need to supply either AWS Key for S3 level encryption or a public key for GPG encryption or both!. Insufficient key material to perform safe encryption.")
	}

	if options.Org == "" {
	    panic("A Org must be provided.")
	}

	if options.Bucket == "" {
		panic("A bucket must be provided.")
	}

    if options.Directory == "" && options.ShareFromList == "" {
        panic("Need to supply a destination for the files to encrypt.  Should be a local path to the source files or an index csv with paths defined within.")
    }

    if options.Directory != "" && options.ShareFromList != "" {
        panic("Do not use both the '--directory' paramter and '--share-from-list' parameter, as their behavior is exclusive.")
    }

    if options.Directory == "/" {
        panic("Input directory cannot be root!")
    }

	if !strings.Contains(strings.ToLower(options.Prefix), "clinical") && !strings.Contains(strings.ToLower(options.Prefix), "documents") && !strings.Contains(strings.ToLower(options.Prefix), "imaging") && !strings.Contains(strings.ToLower(options.Prefix), "molecular"){
	    panic("Prefix command line argument must contain 'clinical' or 'documents' to abide by our lambda trigger!")
	}
}

func init() {
    log.Debug("Initializing share command...")

	rootCmd.AddCommand(shareCmd)

    // core flags
	shareCmd.PersistentFlags().String("directory", "", "The directory to zip, encrypt and share.")
	shareCmd.PersistentFlags().String("org", "", "The Org that owns the files.")
	shareCmd.MarkFlagRequired("org")
	shareCmd.PersistentFlags().String("prefix", "", "A prefix for the S3 path. Currently used to separate clinical and documents files.")

    // technical configuration
	shareCmd.PersistentFlags().Int("parallelism", 10, "The maximum number of files to download and decrypt at a time within a batch.")
	shareCmd.PersistentFlags().Int("chunk-size", 10000, "Files are uploaded and archived in chunks of this size. In case of errors midrun, the latest factor of this number would be present and valid in s3. Many chunks make up a batch.")
	shareCmd.PersistentFlags().Int("batch-size", 100000, "The s3 location increments after every factor of this number. Serves as a cap on batch sizes downstream. A batch is uploaded in many chunks.")
	shareCmd.PersistentFlags().Bool("lambda-trigger", true, "Will send a trigger file to the S3 bucket upon both process completion (when all valid files in the input directory are uploaded) and each internal S3 bucket tie off.")
	shareCmd.PersistentFlags().String("aws-profile", "", "AWS Profile to use for the session.")

    // optional file / file-path configurations
    shareCmd.PersistentFlags().String("scratch-directory", "", "If provided, serves as location where .zip & .gpg files are written to. Is automatically suffixed by org argument. Intended to be leveraged if location will have superior write/read performance. If not provided, .zip and .gpg files are written to the original directory.")
    shareCmd.PersistentFlags().String("archive-directory", "", "If provided, contents of upload directory are moved here after each batch.")
    shareCmd.PersistentFlags().String("metadata-files", "", "If provided, these files are the first to be uploaded and the last to be archived out of the input directory. Comma-separated. I.E. --metadata-files=file1,file2,file3")
    shareCmd.PersistentFlags().Bool("delete-on-completion", true, "If provided, provided directory will be deleted upon the upload of the files.")
    shareCmd.PersistentFlags().String("share-from-list", "", "Local path and filename for encrypting files directly from a CSV index.")
	shareCmd.PersistentFlags().String("aws-role-arn", "", "AWS Role ARN to assume for the session.")

    // ssm key options
	shareCmd.PersistentFlags().String("awskey", "", "The agreed upon S3 key to encrypt data with at the bucket.")
	shareCmd.PersistentFlags().String("receiver-public-key", "", "The receiver's public key.  A local file path.")
	shareCmd.PersistentFlags().String("ssm-public-key", "", "The receiver's public key.  A local file path.")
    shareCmd.PersistentFlags().Bool("is-gcs", false, "Boolean to determine whether to use GCS. Defaults to false.")

	viper.BindPFlag("directory", shareCmd.PersistentFlags().Lookup("directory"))
	viper.BindPFlag("org", shareCmd.PersistentFlags().Lookup("org"))
	viper.BindPFlag("prefix", shareCmd.PersistentFlags().Lookup("prefix"))
	viper.BindPFlag("parallelism", shareCmd.PersistentFlags().Lookup("parallelism"))
	viper.BindPFlag("chunk-size", shareCmd.PersistentFlags().Lookup("chunk-size"))
	viper.BindPFlag("batch-size", shareCmd.PersistentFlags().Lookup("batch-size"))
	viper.BindPFlag("lambda-trigger", shareCmd.PersistentFlags().Lookup("lambda-trigger"))
	viper.BindPFlag("scratch-directory", shareCmd.PersistentFlags().Lookup("scratch-directory"))
	viper.BindPFlag("archive-directory", shareCmd.PersistentFlags().Lookup("archive-directory"))
	viper.BindPFlag("metadata-files", shareCmd.PersistentFlags().Lookup("metadata-files"))
	viper.BindPFlag("awskey", shareCmd.PersistentFlags().Lookup("awskey"))
	viper.BindPFlag("receiver-public-key", shareCmd.PersistentFlags().Lookup("receiver-public-key"))
	viper.BindPFlag("ssm-public-key", shareCmd.PersistentFlags().Lookup("ssm-public-key"))
	viper.BindPFlag("is-gcs", shareCmd.PersistentFlags().Lookup("is-gcs"))
	viper.BindPFlag("aws-profile", shareCmd.PersistentFlags().Lookup("aws-profile"))
	viper.BindPFlag("delete-on-completion", shareCmd.PersistentFlags().Lookup("delete-on-completion"))
    viper.BindPFlag("share-from-list", shareCmd.PersistentFlags().Lookup("share-from-list"))
	viper.BindPFlag("aws-role-arn", shareCmd.PersistentFlags().Lookup("aws-role-arn"))

	//log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{})
	log.SetLevel(log.DebugLevel)
}
