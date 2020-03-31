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
	"io/ioutil"
	"net/http"
	"path"

	"github.com/pkg/errors"
	"goji.io/v3/pat"

	"github.com/uncharted-distil/distil-compute/metadata"
	"github.com/uncharted-distil/distil-pipeline-executer/dataset"
	"github.com/uncharted-distil/distil-pipeline-executer/env"
	"github.com/uncharted-distil/distil-pipeline-executer/task"
	"github.com/uncharted-distil/distil-pipeline-executer/util"
	log "github.com/unchartedsoftware/plog"
)

// Prediction is a result from a produce call.
type Prediction struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

// ProduceHandler takes in unlabelled data and generates predictions using
// a fitted model.
func ProduceHandler(config *env.Config) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineID := pat.Param(r, "pipeline-id")
		log.Infof("produce request received for pipeline '%s'", pipelineID)
		//typ := pat.Param(r, "type")
		//format := pat.Param(r, "format")

		// parse the input data
		requestBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			handleError(w, errors.Wrapf(err, "unable to read request body"))
			return
		}
		defer r.Body.Close()

		datasetType, err := task.GetDatasetType(pipelineID)
		if err != nil {
			handleError(w, err)
			return
		}

		var ds task.DatasetConstructor
		switch datasetType {
		case dataset.ImageType:
			ds, err = dataset.NewImageDataset(requestBody)
		case dataset.TableType:
			ds, err = dataset.NewTableDataset(requestBody)
		case dataset.UnknownType:
			err = errors.New("unsupproted dataset type")
		}
		if err != nil {
			handleError(w, err)
			return
		}

		// create the dataset to be used for the produce call
		schemaPath, err := task.CreateDataset(pipelineID, ds)
		if err != nil {
			handleError(w, err)
			return
		}

		data, err := readData(schemaPath)
		if err != nil {
			handleError(w, err)
			return
		}

		queue := task.NewQueue()
		queue.AddDataset(ds.GetPredictionsID())
		for _, r := range data {
			queue.AddEntry(ds.GetPredictionsID(), r)
		}

		// run predictions on the newly created dataset
		predictions, err := task.ProduceBatch(pipelineID, schemaPath, ds.GetPredictionsID(), queue, config)
		if err != nil {
			handleError(w, err)
			return
		}

		// create the prediction output (skipping header)
		output := make([]*Prediction, 0)
		for _, p := range predictions {
			output = append(output, &Prediction{
				ID:    p[0],
				Value: p[1],
			})
		}

		if config.ClearDataset {
			err = task.ClearDataset(pipelineID, ds.GetPredictionsID())
			if err != nil {
				handleError(w, errors.Wrap(err, "unable to read produce output"))
				return
			}
		}

		err = handleJSON(w, map[string]interface{}{
			"pipelineId":   pipelineID,
			"predictionId": ds.GetPredictionsID(),
			"predictions":  output,
		})
		if err != nil {
			handleError(w, errors.Wrap(err, "unable marshal produce result into JSON"))
			return
		}
	}
}

func readData(schemaFilename string) ([][]string, error) {
	meta, err := metadata.LoadMetadataFromOriginalSchema(schemaFilename, false)
	if err != nil {
		return nil, err
	}

	mainDR := meta.GetMainDataResource()
	dateFilename := path.Join(path.Dir(schemaFilename), mainDR.ResPath)

	data, err := util.ReadCSVFile(dateFilename, true)
	if err != nil {
		return nil, err
	}

	return data, nil
}
