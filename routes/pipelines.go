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
	"net/http"
	"path"
	"time"

	"github.com/pkg/errors"

	"github.com/uncharted-distil/distil-compute/metadata"
	"github.com/uncharted-distil/distil-pipeline-executer/env"
	"github.com/uncharted-distil/distil-pipeline-executer/util"
)

// PipelineInfo represents a pipeline that can be used for fitting or producing.
type PipelineInfo struct {
	PipelineID        string    `json:"pipelineId"`
	DatasetID         string    `json:"datasetId"`
	UploadedTimestamp time.Time `json:"uploadedTimestamp"`
	Fitted            bool      `json:"fitted"`
	FittedTimestamp   time.Time `json:"fittedTimestamp"`
}

// PipelinesHandler returns the list of pipelines found in the configured folder.
func PipelinesHandler(config env.Config) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelines, err := getPipelines(config.PipelineDir)
		if err != nil {
			handleError(w, errors.Wrapf(err, "unable to get pipelines from directory '%s'", config.PipelineDir))
			return
		}

		err = handleJSON(w, pipelines)
		if err != nil {
			handleError(w, errors.Wrap(err, "unable marshal version into JSON and write response"))
			return
		}
	}
}

func getPipelines(directory string) ([]*PipelineInfo, error) {
	// a pipeline will be a folder with a dataset doc and a pipeline.d3m file
	// get all folders in the pipeline folder
	directories, err := util.GetDirectories(directory)
	if err != nil {
		return nil, err
	}

	// only consider the folders that have the required dataset doc and pipeline files
	pipelines := make([]*PipelineInfo, 0)
	for _, d := range directories {
		isPipeline, isFit := util.IsPipelineDirectory(d)
		if isPipeline {
			meta, err := metadata.LoadMetadataFromOriginalSchema(path.Join(d, "datasetDoc.json"))
			if err != nil {
				return nil, err
			}

			modTime, _ := util.GetLastModifiedTime(path.Join(d, "pipeline.json"))
			fitTime := time.Time{}
			if isFit {
				fitTime, _ = util.GetLastModifiedTime(path.Join(d, "pipeline.json"))
			}

			pipelines = append(pipelines, &PipelineInfo{
				PipelineID:        path.Base(d),
				Fitted:            isFit,
				UploadedTimestamp: modTime,
				FittedTimestamp:   fitTime,
				DatasetID:         meta.ID,
			})
		}
	}

	return pipelines, nil
}
