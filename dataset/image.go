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

package dataset

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"os"
	"path"

	"github.com/pkg/errors"

	cm "github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-pipeline-executer/model"
	"github.com/uncharted-distil/distil-pipeline-executer/util"
)

// Image captures the data in an image dataset.
type Image struct {
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

// NewImageDataset creates a new image dataset from raw byte data, assuming json.
func NewImageDataset(rawData []byte) (*Image, error) {
	images := &Image{}
	err := json.Unmarshal(rawData, images)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse json")
	}

	return images, nil
}

// CreateDataset creates a basic dataset from an image dataset
func (i *Image) CreateDataset(rootPath string) (*model.Dataset, error) {
	learningData := make([][]string, len(i.Images))
	mediaPath := path.Join(rootPath, "media")
	for index, im := range i.Images {
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
		imagePath := path.Join(mediaPath, imageName)
		err = util.WriteFileWithDirs(imagePath, imageRaw, os.ModePerm)
		if err != nil {
			return nil, err
		}

		// add the relevant row to the learning data
		learningData[index] = []string{im.ID, imageName, im.Label}
	}

	dataset := &model.Dataset{
		ID:        i.ID,
		Variables: []string{cm.D3MIndexName, "image_file", "label"},
		Data:      learningData,
	}
	return dataset, nil
}

// GetPredictionsID returns the prediction set id.
func (i *Image) GetPredictionsID() string {
	return i.ID
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
