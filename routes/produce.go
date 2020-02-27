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
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io/ioutil"
	"net/http"
	"os"
	"path"

	"github.com/pkg/errors"
	"goji.io/v3/pat"

	cm "github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-pipeline-executer/env"
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

// Prediction is a result from a produce call.
type Prediction struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

// CreateDataset creates a basic dataset from an image dataset
func (i *ImageDataset) CreateDataset(rootPath string) (*model.Dataset, error) {
	learningData := make([][]string, 0)
	for _, im := range i.Images {
		// read the image into memory
		img, err := im.read()
		if err != nil {
			return nil, err
		}
		imageRaw, err := toJPEG(&img)
		if err != nil {
			return nil, err
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

func (i *ImageEncoded) read() (image.Image, error) {
	// decode the image
	imageRaw, err := base64.StdEncoding.DecodeString(i.Image)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to decode image '%s'", i.ID)
	}

	switch i.Type {
	case "png":
		return png.Decode(bytes.NewReader(imageRaw))
	case "jpg", "jpeg":
		return jpeg.Decode(bytes.NewReader(imageRaw))
	default:
		return nil, errors.Errorf("unsupported image type '%s'", i.Type)
	}
}

func toJPEG(img *image.Image) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := jpeg.Encode(buf, *img, nil); err != nil {
		return nil, errors.Wrap(err, "unable to encode jpg")
	}

	return buf.Bytes(), nil
}

func toPNG(img *image.Image) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := png.Encode(buf, *img); err != nil {
		return nil, errors.Wrap(err, "unable to encode png")
	}

	return buf.Bytes(), nil
}

// ProduceHandler takes in unlabelled data and generates predictions using
// a fitted model.
func ProduceHandler(config *env.Config) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineID := pat.Param(r, "pipeline-id")
		//typ := pat.Param(r, "type")
		//format := pat.Param(r, "format")

		// parse the input data
		requestBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			handleError(w, errors.Wrapf(err, "unable to read request body"))
			return
		}
		defer r.Body.Close()

		images := &ImageDataset{}
		err = json.Unmarshal(requestBody, images)
		if err != nil {
			handleError(w, errors.Wrapf(err, "unable to parse json"))
			return
		}

		// create the dataset to be used for the produce call
		schemaPath, err := task.CreateDataset(pipelineID, images.ID, images)
		if err != nil {
			handleError(w, err)
			return
		}

		// run predictions on the newly created dataset
		outputFile, err := task.Produce(pipelineID, schemaPath, images.ID, config)
		if err != nil {
			handleError(w, err)
			return
		}

		// read the output
		file, err := os.Open(outputFile)
		if err != nil {
			handleError(w, err)
			return
		}
		reader := csv.NewReader(file)
		predictionsRaw, err := reader.ReadAll()
		if err != nil {
			handleError(w, errors.Wrap(err, "unable to read produce output"))
			return
		}

		// create the prediction output (skipping header)
		output := make([]*Prediction, 0)
		for _, p := range predictionsRaw[1:] {
			output = append(output, &Prediction{
				ID:    p[0],
				Value: p[1],
			})
		}

		err = handleJSON(w, map[string]interface{}{
			"pipelineID":  pipelineID,
			"predictions": output,
		})
		if err != nil {
			handleError(w, errors.Wrap(err, "unable marshal produce result into JSON"))
			return
		}
	}
}
