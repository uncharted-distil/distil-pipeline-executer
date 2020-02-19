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
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
)

// IsPipelineDirectory checks if a given directory is a D3M pipeline directory
// and if it is fit.
func IsPipelineDirectory(directory string) (bool, bool) {
	// a pipeline directory has a pipeline.d3m and datasetDoc.json files
	isPipeline := true
	isFit := false
	if _, err := os.Stat(path.Join(directory, "datasetDoc.json")); os.IsNotExist(err) {
		isPipeline = false
	}
	if _, err := os.Stat(path.Join(directory, "pipeline.json")); os.IsNotExist(err) {
		isPipeline = false
	}
	if _, err := os.Stat(path.Join(directory, "pipeline.d3m")); err == nil {
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
