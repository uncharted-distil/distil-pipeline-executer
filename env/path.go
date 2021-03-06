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

package env

import (
	"path"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
)

var (
	pipelinePath     = ""
	pipelineJSONName = ""
	pipelineD3MName  = ""
	predictionPath   = ""
	problemPath      = ""
	datasetPath      = ""

	initialized = false
)

// Initialize initializes the paths used in the application.
func Initialize(config *Config) error {
	log.Infof("initializing paths based on configuration")
	if initialized {
		return errors.Errorf("path resolution already initialized")
	}

	pipelinePath = config.PipelineDir
	pipelineJSONName = config.PipelineJSON
	pipelineD3MName = config.PipelineD3M
	problemPath = config.ProblemFile
	datasetPath = config.DatasetDir
	predictionPath = config.PredictionDir

	log.Infof("using '%s' as dataset path", datasetPath)
	log.Infof("using '%s' as prediction path", predictionPath)
	log.Infof("using '%s' as pipeline path", pipelinePath)
	log.Infof("using '%s' as pipeline json name", pipelineJSONName)
	log.Infof("using '%s' as pipeline d3m name", pipelineD3MName)

	initialized = true

	return nil
}

// ResolvePipelinePath returns the path to the folder containing the pipeline info.
func ResolvePipelinePath(pipelineID string) string {
	return path.Join(pipelinePath, pipelineID)
}

// ResolvePipelineJSONPath returns the path to the json file representing the pipeline.
func ResolvePipelineJSONPath(pipelineID string) string {
	return path.Join(pipelinePath, pipelineID, pipelineJSONName)
}

// ResolveProblemPath returns the path to the file representing the
// problem for the pipeline.
func ResolveProblemPath(pipelineID string) string {
	return path.Join(pipelinePath, pipelineID, problemPath)
}

// ResolvePipelineD3MPath returns the path pickled fitted pipeline.
func ResolvePipelineD3MPath(pipelineID string) string {
	return path.Join(pipelinePath, pipelineID, pipelineD3MName)
}

// ResolveDatasetPath returns the path for a dataset folder
func ResolveDatasetPath(datasetID string) string {
	return path.Join(datasetPath, datasetID)
}

// ResolvePredictionPath returns the path for a prediction folder
func ResolvePredictionPath(predictionID string) string {
	return path.Join(predictionPath, predictionID)
}
