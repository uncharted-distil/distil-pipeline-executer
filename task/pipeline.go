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

package task

import (
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"

	"github.com/uncharted-distil/distil-compute/metadata"
	"github.com/uncharted-distil/distil-compute/primitive/compute"
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

// GetPipelines returns a list of pipelines that exist at the specified location.
func GetPipelines(directory string) ([]*PipelineInfo, error) {
	log.Infof("getting pipelines found in '%s'", directory)
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
			meta, err := metadata.LoadMetadataFromOriginalSchema(path.Join(d, "datasetDoc.json"), false)
			if err != nil {
				return nil, err
			}

			modTime, _ := util.GetLastModifiedTime(path.Join(d, "pipeline.json"))
			fitTime := time.Time{}
			if isFit {
				fitTime, _ = util.GetLastModifiedTime(path.Join(d, "pipeline.d3m"))
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
	log.Infof("done building pipeline listing")

	return pipelines, nil
}

// StorePipeline stores a pipeline to disk for future use.
func StorePipeline(pipelineID string, pipeline []byte, datasetSchema []byte, problem []byte, overwrite bool) error {
	log.Infof("storing pipeline with id '%s'", pipelineID)
	pipelineFolder := env.ResolvePipelinePath(pipelineID)
	schemaPath := path.Join(pipelineFolder, compute.D3MDataSchema)
	pipelinePath := env.ResolvePipelineJSONPath(pipelineID)
	problemPath := env.ResolveProblemPath(pipelineID)

	// check if already there and if not set to overwrite then error
	if util.FileExists(schemaPath) {
		if !overwrite {
			return errors.Errorf("pipeline '%s' already exists", pipelineID)
		}

		// remove existing pipeline and recreate folder
		log.Infof("removing pipeline '%s'", pipelineID)
		err := util.RemoveContents(pipelineFolder)
		if err != nil {
			return err
		}
	}

	// write out the schema and pipeline data
	log.Infof("writing schema, problem and pipeline for id '%s'", pipelineID)
	err := util.WriteFileWithDirs(schemaPath, datasetSchema, os.ModePerm)
	if err != nil {
		return err
	}
	err = util.WriteFileWithDirs(problemPath, problem, os.ModePerm)
	if err != nil {
		return err
	}
	err = util.WriteFileWithDirs(pipelinePath, pipeline, os.ModePerm)
	if err != nil {
		return err
	}
	log.Infof("done writing out pipeline '%s'", pipelineID)

	return nil
}
