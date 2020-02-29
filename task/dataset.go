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
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"path"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"

	"github.com/uncharted-distil/distil-compute/metadata"
	cm "github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-compute/primitive/compute"
	"github.com/uncharted-distil/distil-pipeline-executer/env"
	"github.com/uncharted-distil/distil-pipeline-executer/model"
	"github.com/uncharted-distil/distil/api/util"
)

// DatasetConstructor is used to build a dataset.
type DatasetConstructor interface {
	CreateDataset(rootPath string) (*model.Dataset, error)
}

// CreateDataset creates a dataset that can be used for fitting a pipeline or
// producing predictions from a pipeline.
func CreateDataset(pipelineID string, predictionsID string, datasetCtor DatasetConstructor) (string, error) {
	log.Infof("creating dataset for pipeline '%s' using prediction id '%s'", pipelineID, predictionsID)
	// create the raw dataset from the input
	datasetPath := env.ResolveDatasetPath(predictionsID)
	dataset, err := datasetCtor.CreateDataset(datasetPath)
	if err != nil {
		return "", err
	}

	// create the predictions folder
	log.Infof("created predictions folder for prediction '%s'", predictionsID)
	predictionsFolder := env.ResolvePredictionPath(predictionsID)
	os.Mkdir(predictionsFolder, os.ModePerm)

	// read the source schema doc
	pipelinePath := env.ResolvePipelinePath(pipelineID)
	pipelineSchemaDoc := path.Join(pipelinePath, compute.D3MDataSchema)
	meta, err := metadata.LoadMetadataFromOriginalSchema(pipelineSchemaDoc, false)
	if err != nil {
		return "", err
	}

	// augment the dataset to match raw dataset columns to dataset doc variables
	mainDR := meta.GetMainDataResource()
	augmentedData, err := augmentPredictionDataset(dataset, mainDR.Variables)
	if err != nil {
		return "", err
	}

	// store formatted dataset
	log.Infof("storing formatted dataset to '%s'", datasetPath)
	outputBytes := &bytes.Buffer{}
	writerOutput := csv.NewWriter(outputBytes)
	err = writerOutput.WriteAll(augmentedData)
	if err != nil {
		return "", errors.Wrapf(err, "unable to write augmented data")
	}
	writerOutput.Flush()
	err = util.WriteFileWithDirs(path.Join(datasetPath, mainDR.ResPath), outputBytes.Bytes(), os.ModePerm)
	if err != nil {
		return "", errors.Wrapf(err, "unable to write augmented data to disk")
	}

	// store updated metadata
	outputSchemaPath := path.Join(datasetPath, compute.D3MDataSchema)
	err = metadata.WriteSchema(meta, outputSchemaPath, false)
	if err != nil {
		return "", err
	}

	return outputSchemaPath, nil
}

func augmentPredictionDataset(dataset *model.Dataset, variables []*cm.Variable) ([][]string, error) {
	log.Infof("augmenting data fields with schema variables")

	// map fields to indices
	headerSource := make([]string, len(variables))
	sourceVariableMap := make(map[string]*cm.Variable)
	for _, v := range variables {
		sourceVariableMap[v.DisplayName] = v
		headerSource[v.Index] = v.DisplayName
	}

	addIndex := true
	predictVariablesMap := make(map[int]int)
	for i, pv := range dataset.Variables {
		if sourceVariableMap[pv] != nil {
			predictVariablesMap[i] = sourceVariableMap[pv].Index
			log.Infof("mapped '%s' to index %d", pv, predictVariablesMap[i])
		} else {
			predictVariablesMap[i] = -1
			log.Warnf("field '%s' not found in source dataset", pv)
		}

		if pv == cm.D3MIndexName {
			addIndex = false
		}
	}

	// write the header
	output := make([][]string, len(dataset.Data)+1)
	output[0] = headerSource

	// read the rest of the data
	log.Infof("rewriting inference dataset to match source dataset structure")
	count := 0
	d3mFieldIndex := sourceVariableMap[cm.D3MIndexName].Index
	for _, line := range dataset.Data {
		// write the columns in the same order as the source dataset
		outputLine := make([]string, len(sourceVariableMap))
		for i, f := range line {
			sourceIndex := predictVariablesMap[i]
			if sourceIndex >= 0 {
				outputLine[sourceIndex] = f
			}
		}

		if addIndex {
			outputLine[d3mFieldIndex] = fmt.Sprintf("%d", count)
		}
		count = count + 1
		output[count] = outputLine
	}

	log.Infof("done augmenting inference dataset")

	return output, nil
}

// ClearDataset deletes the dataset and prediction data.
func ClearDataset(pipelineID string, predictionID string) error {
	log.Infof("clearing dataset and prediction info for pipeline '%s' and prediction '%s'", pipelineID, predictionID)
	predictionsDir := env.ResolvePredictionPath(predictionID)
	datasetDir := env.ResolveDatasetPath(predictionID)

	// delete dataset directory & prediction directory
	log.Infof("deleting prediction content found in '%s'", predictionsDir)
	err := util.RemoveContents(predictionsDir)
	if err != nil {
		return err
	}
	log.Infof("deleting dataset content found in '%s'", datasetDir)
	err = util.RemoveContents(datasetDir)
	if err != nil {
		return err
	}
	return nil
}
