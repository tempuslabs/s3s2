package zip

import (
	"archive/zip"
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
	utils "github.com/tempuslabs/s3s2/utils"
)

// ZipFile zips the provided file.
func ZipFile(InputFn string, OutputFn string, directory string) string {

    log.Debugf("Zipping file '%s' to '%s'", InputFn, OutputFn)

    dir, _ := filepath.Split(OutputFn)

    os.MkdirAll(dir, os.ModePerm)

	newZipFile, err := os.Create(OutputFn)
	utils.PanicIfError("Unable to create zip file - ", err)
	defer newZipFile.Close()

	zipWriter := zip.NewWriter(newZipFile)
	defer zipWriter.Close()

	zipfile, err := os.Open(InputFn)
	utils.PanicIfError("Unable to open zip file location - ", err)
	defer zipfile.Close()

	// Get the file information
	info, err := zipfile.Stat()
	utils.PanicIfError("Unable to get zip file information - ", err)

	header, err := zip.FileInfoHeader(info)
	utils.PanicIfError("Unable to get zip file header info - ", err)

	// Using FileInfoHeader() above only uses the basename of the file. If we want
	// to preserve the folder structure we can overwrite this with the full path.
	header.Name = strings.Replace(InputFn, directory, "", -1)

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	utils.PanicIfError("Unable to create header info - ", err)

	if _, err = io.Copy(writer, zipfile); err != nil {
		log.Error(err)
	}

	return OutputFn
}

// ZipFile zips the provided file.
func ZipFileInMemory(InputFn string, date_folder string) *bytes.Buffer {

	log.Debugf("Zipping file '%s' in memory", InputFn)

	source, err := os.Open(InputFn)
	utils.PanicIfError("Unable to open source file - ", err)
	defer source.Close()

	_, file_name := filepath.Split(InputFn)
	// create output buffer
	buf := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buf)

	f, err := zipWriter.Create(filepath.Join(date_folder, file_name))
	utils.PanicIfError("Unable to create writer - ", err)

	data, err := ioutil.ReadAll(source)
	utils.PanicIfError("Unable to write to file to bytes - ", err)

	_, err = f.Write(data)
	utils.PanicIfError("Unable to write to file writer - ", err)

	err = zipWriter.Close()
	utils.PanicIfError("Unable to close writer - ", err)

	return buf
}

// UnZipFile uncompresses and archive
func UnZipFile(InputFn string, OutputFn string, directory string) string {

	if !strings.HasSuffix(InputFn, ".zip") {
		log.Warnf("Skipping file because it is not a zip file, %s", OutputFn)
		return OutputFn
	}

	zReader, err := zip.OpenReader(InputFn)
    utils.PanicIfError("Unable to open zipreader - ", err)
	defer zReader.Close()

	for _, file := range zReader.Reader.File {

		zippedFile, err := file.Open()
        utils.PanicIfError("Unable to open zipped file - ", err)
		defer zippedFile.Close()

		extractedFilePath := filepath.Join(directory, OutputFn)

		if file.FileInfo().IsDir() {
			os.MkdirAll(extractedFilePath, file.Mode())
			log.Debugf("Directory Created: '%s'", extractedFilePath)
		} else {

			extractDir := filepath.Dir(extractedFilePath)
			os.MkdirAll(extractDir, os.ModePerm)

			outputFile, err := os.OpenFile(
				extractedFilePath,
				os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
				file.Mode(),
			)
            utils.PanicIfError("Unable to open zipreader - ", err)
            log.Debugf("\tFile extracted to: '%s'", extractedFilePath)

			_, err = io.Copy(outputFile, zippedFile)
			utils.PanicIfError("Unable to create zipped file - ", err)

			outputFile.Close()
		}
	}
	return OutputFn
}
