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

package routes

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"

	"github.com/pkg/errors"
	"goji.io/v3/pat"

	cm "github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-pipeline-executer/model"
	"github.com/uncharted-distil/distil-pipeline-executer/task"
	"github.com/uncharted-distil/distil-pipeline-executer/util"
)

// ImageDataset captures the data in an image dataset.
type ImageDataset struct {
	ID     string          `json:"id"`
	Images []*ImageEncoded `json:"images"`
}

// ImageEncoded is a base46 encoded image.
type ImageEncoded struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Image string `json:"image"`
	Label string `json:"label"`
}

// CreateDataset creates a basic dataset from an image dataset
func (i *ImageDataset) CreateDataset(rootPath string) (*model.Dataset, error) {
	learningData := make([][]string, 0)
	for _, im := range i.Images {
		// decode the image
		imageRaw, err := base64.StdEncoding.DecodeString(im.Image)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to decode image '%s'", im.ID)
		}

		// store it to disk
		imageName := fmt.Sprintf("%s.%s", im.ID, im.Type)
		imagePath := path.Join(rootPath, "media", imageName)
		err = util.WriteFileWithDirs(imagePath, imageRaw, os.ModePerm)
		if err != nil {
			return nil, err
		}

		// add the relevant row to the learning data
		learningData = append(learningData, []string{im.ID, imageName, im.Label})
	}

	dataset := &model.Dataset{
		Variables: []string{cm.D3MIndexName, "image_file", "label"},
		Data:      learningData,
	}
	return dataset, nil
}

// ProduceHandler takes in unlabelled data and generates predictions using
// a fitted model.
func ProduceHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineID := pat.Param(r, "pipeline-id")
		//typ := pat.Param(r, "type")
		//format := pat.Param(r, "format")

		// parse the input data
		requestBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			handleError(w, err)
			return
		}
		defer r.Body.Close()

		var images *ImageDataset
		err = json.Unmarshal(requestBody, images)
		if err != nil {
			handleError(w, err)
			return
		}

		// create the dataset to be used for the produce call
		schemaPath, err := task.CreateDataset(pipelineID, images.ID, images)
		if err != nil {
			handleError(w, err)
			return
		}

		// run predictions on the newly created dataset
		err = task.Produce(pipelineID, schemaPath, images.ID)
		if err != nil {
			handleError(w, err)
			return
		}
	}
}
