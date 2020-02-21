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
	"github.com/uncharted-distil/distil-pipeline-executer/task"
	"goji.io/v3/pat"
)

// PipelineUpload contains the necessary info to upload a pipeline for future use.
type PipelineUpload struct {
	DatasetSchema json.RawMessage `json:"datasetSchema"`
	Pipeline      json.RawMessage `json:"pipeline"`
	Problem       json.RawMessage `json:"problem"`
}

// UploadHandler stores a pipeline json file and matching dataset document
// to the pipelines folder.
func UploadHandler(pipelinesDir string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineID := pat.Param(r, "pipeline-id")

		// need the pipeline in json form as well as the full dataset doc
		requestBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			handleError(w, err)
			return
		}
		defer r.Body.Close()

		var upload PipelineUpload
		err = json.Unmarshal(requestBody, &upload)
		if err != nil {
			handleError(w, err)
			return
		}

		if upload.DatasetSchema == nil {
			handleError(w, errors.Errorf("dataset schema not provided in upload"))
			return
		}
		if upload.Pipeline == nil {
			handleError(w, errors.Errorf("pipeline not provided in upload"))
			return
		}
		if upload.Problem == nil {
			handleError(w, errors.Errorf("problem not provided in upload"))
			return
		}

		// write out the schema, pipeline and problem in the proper folders
		pipelineJSON, err := upload.Pipeline.MarshalJSON()
		if err != nil {
			handleError(w, errors.Wrap(err, "unable to marshal pipeline json"))
			return
		}
		schemaJSON, err := upload.DatasetSchema.MarshalJSON()
		if err != nil {
			handleError(w, errors.Wrap(err, "unable to marshal schema json"))
			return
		}
		problemJSON, err := upload.Problem.MarshalJSON()
		if err != nil {
			handleError(w, errors.Wrap(err, "unable to marshal problem json"))
			return
		}

		err = task.StorePipeline(pipelineID, pipelineJSON, schemaJSON, problemJSON, true)
		if err != nil {
			handleError(w, err)
			return
		}

		err = handleJSON(w, map[string]interface{}{
			"pipelineID": pipelineID,
			"result":     "success",
		})
		if err != nil {
			handleError(w, errors.Wrap(err, "unable marshal upload result into JSON"))
			return
		}
	}
}
