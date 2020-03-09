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
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
	"goji.io/v3/pat"

	"github.com/uncharted-distil/distil-pipeline-executer/env"
	"github.com/uncharted-distil/distil-pipeline-executer/task"
)

// FitHandler takes in labelled data and trains the specified pipeline.
func FitHandler(config *env.Config) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineID := pat.Param(r, "pipeline-id")
		log.Infof("fit request received for pipeline '%s'", pipelineID)
		//typ := pat.Param(r, "type")
		//format := pat.Param(r, "format")

		// parse the input data
		requestBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			handleError(w, errors.Wrapf(err, "unable to read request body"))
			return
		}
		defer r.Body.Close()

		log.Infof("unmarshalling request body")
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
		err = task.Fit(pipelineID, schemaPath, images.ID, config)
		if err != nil {
			handleError(w, err)
			return
		}

		err = handleJSON(w, map[string]interface{}{
			"pipelineId":   pipelineID,
			"predictionId": images.ID,
			"fitted":       true,
		})
		if err != nil {
			handleError(w, errors.Wrap(err, "unable marshal produce result into JSON"))
			return
		}
	}
}
