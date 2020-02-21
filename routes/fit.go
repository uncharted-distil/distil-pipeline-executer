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

	"goji.io/v3/pat"

	"github.com/uncharted-distil/distil-pipeline-executer/task"
)

// FitHandler takes in labelled data and trains the specified pipeline.
func FitHandler() func(http.ResponseWriter, *http.Request) {
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
		err = task.Fit(pipelineID, schemaPath, images.ID)
		if err != nil {
			handleError(w, err)
			return
		}
	}
}
