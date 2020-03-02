//
//   Copyright © 2020 Uncharted Software Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package util

import (
	"encoding/csv"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/uncharted-distil/distil-compute/primitive/compute"
	"github.com/uncharted-distil/distil-pipeline-executer/env"
)

var (
	config *env.Config
)

// SetConfig sets the configuration values to use.
func SetConfig(c *env.Config) {
	config = c
}

// IsPipelineDirectory checks if a given directory is a D3M pipeline directory
// and if it is fit.
func IsPipelineDirectory(directory string) (bool, bool) {
	// a pipeline directory has a pipeline.d3m and datasetDoc.json files
	isPipeline := true
	isFit := false
	if _, err := os.Stat(path.Join(directory, compute.D3MDataSchema)); os.IsNotExist(err) {
		isPipeline = false
	}
	if _, err := os.Stat(path.Join(directory, config.ProblemFile)); os.IsNotExist(err) {
		isPipeline = false
	}
	if _, err := os.Stat(path.Join(directory, config.PipelineJSON)); os.IsNotExist(err) {
		isPipeline = false
	}
	if _, err := os.Stat(path.Join(directory, config.PipelineD3M)); err == nil {
		isFit = true
	}

	return isPipeline, isFit
}

// GetDirectories returns a list of directories found using the supplied path.
func GetDirectories(inputPath string) ([]string, error) {
	files, err := ioutil.ReadDir(inputPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list directory content")
	}

	dirs := make([]string, 0)
	for _, f := range files {
		if f.IsDir() {
			dirs = append(dirs, path.Join(inputPath, f.Name()))
		}
	}

	return dirs, nil
}

// GetLastModifiedTime returns the last time the file was modified
func GetLastModifiedTime(file string) (time.Time, error) {
	fi, err := os.Stat(file)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "unable to read file '%s'", file)
	}

	return fi.ModTime(), nil
}

// WriteFileWithDirs writes the file and creates any missing directories along
// the way.
func WriteFileWithDirs(filename string, data []byte, perm os.FileMode) error {

	dir, _ := filepath.Split(filename)

	// make all dirs up to the destination
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "unable to make required directory")
	}

	// write the file
	return ioutil.WriteFile(filename, data, perm)
}

// FileExists checks if a file already exists on disk.
func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

// RemoveContents removes the files and directories from the supplied parent.
func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return errors.Wrap(err, "unable to open directory")
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return errors.Wrap(err, "unable to read directory contents")
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return errors.Wrap(err, "unable to remove file from directory")
		}
	}
	return nil
}

// ReadCSVFile reads a csv file and returns the string slice representation of the data.
func ReadCSVFile(filename string, hasHeader bool) ([][]string, error) {
	// open the file
	csvFile, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open data file")
	}
	defer csvFile.Close()
	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = 0

	lines := make([][]string, 0)

	// skip the header as needed
	if hasHeader {
		_, err = reader.Read()
		if err != nil {
			return nil, errors.Wrap(err, "failed to read header from file")
		}
	}

	// read the raw data
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			continue
		}

		lines = append(lines, line)
	}

	return lines, nil
}
