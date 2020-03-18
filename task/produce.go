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
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"

	"github.com/uncharted-distil/distil-compute/metadata"
	"github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-compute/primitive/compute"
	"github.com/uncharted-distil/distil-pipeline-executer/env"
	"github.com/uncharted-distil/distil-pipeline-executer/util"
)

// Produce produces predictions using the specified model and input data.
func Produce(pipelineID string, schemaFile string, predictionsID string, config *env.Config) ([][]string, error) {
	// run the produce command
	log.Infof("running produce command using shell")

	// need to make the output folder for the predictions
	predictionsDir := env.ResolvePredictionPath(predictionsID)
	predictionOutput := path.Join(predictionsDir, "outputs.0.csv")
	err := os.MkdirAll(predictionsDir, os.ModePerm)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create predictions output folder")
	}
	log.Infof("predictions output folder created ('%s')", predictionsDir)

	commandLine := fmt.Sprintf("python3 runner.py runtime -v %s produce -t %s -f %s -o %s",
		config.D3MStaticDir, schemaFile, env.ResolvePipelineD3MPath(pipelineID), predictionOutput)
	cmd := exec.Command("/bin/sh", "-c", commandLine)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	log.Infof("out: %s", stdout.String())
	if err != nil {
		log.Errorf("err: %s", stderr.String())
		return nil, errors.Wrap(err, "unable to run produce command")
	}
	log.Infof("produce output written to '%s'", predictionOutput)

	return util.ReadCSVFile(predictionOutput, true)
}

// ProduceBatch runs the produce command in batches. Predictions are then returned as they complete.
func ProduceBatch(pipelineID string, schemaFile string, predictionsID string, queue *Queue, config *env.Config) ([][]string, error) {
	log.Infof("producing predictions using batches")
	batchSize := config.BatchSize
	rootDatasetPath := env.ResolveDatasetPath(predictionsID)

	meta, err := metadata.LoadMetadataFromOriginalSchema(schemaFile, false)
	if err != nil {
		return nil, err
	}

	output := make([][]string, 0)
	count := 1
	previousThroughput := 10.0
	for {
		batch := queue.RemoveEntries(predictionsID, batchSize)
		if len(batch) == 0 {
			break
		}
		batchID := fmt.Sprintf("batch-%d", count)
		log.Infof("pulled %d entries into a batch using id '%s' (%d remaining)", len(batch), batchID, queue.GetLength(predictionsID))

		// write the batch to disk
		batchPath, err := writeBatch(meta, rootDatasetPath, batchID, batch)
		if err != nil {
			return nil, err
		}

		// remove the leading / from the relative path
		batchPathRelative := strings.Replace(batchPath, rootDatasetPath, "", 1)[1:]

		// update the metadata
		mainDR := meta.GetMainDataResource()
		mainDR.ResPath = batchPathRelative

		// write the metadata for the batch
		err = metadata.WriteSchema(meta, schemaFile, false)
		if err != nil {
			return nil, err
		}

		// produce predictions for the batch
		produceStart := time.Now()
		batchOutput, err := Produce(pipelineID, schemaFile, predictionsID, config)
		if err != nil {
			return nil, err
		}
		produceEnd := time.Now()

		// merge all predictions
		output = append(output, batchOutput...)

		count = count + 1
		currentTimeTaken := produceEnd.Sub(produceStart)
		batchSize, previousThroughput = adjustBatchSize(config, float64(batchSize), currentTimeTaken, previousThroughput)
	}

	return output, nil
}

func writeBatch(meta *model.Metadata, datasetPath string, batchID string, data [][]string) (string, error) {
	// get batch data folder
	batchOutputPath := path.Join(datasetPath, batchID, compute.D3MLearningData)
	log.Infof("storing batch to '%s'", batchOutputPath)
	outputBytes := &bytes.Buffer{}
	writerOutput := csv.NewWriter(outputBytes)
	err := writerOutput.Write(meta.GetMainDataResource().GenerateHeader())
	if err != nil {
		return "", errors.Wrapf(err, "unable to write batch header")
	}

	err = writerOutput.WriteAll(data)
	if err != nil {
		return "", errors.Wrapf(err, "unable to write batch data")
	}
	writerOutput.Flush()
	err = util.WriteFileWithDirs(batchOutputPath, outputBytes.Bytes(), os.ModePerm)
	if err != nil {
		return "", errors.Wrapf(err, "unable to write batch data to disk")
	}

	return batchOutputPath, nil
}

func adjustBatchSize(config *env.Config, currentBatchSize float64, currentTimeTaken time.Duration, previousThroughput float64) (int, float64) {
	currentThroughput := currentBatchSize / currentTimeTaken.Seconds()
	if currentThroughput > previousThroughput {
		newSize := int(currentBatchSize * config.BatchSizeIncreaseFactor)
		log.Infof("latest batch had higher throughput (%v) than previous batch (%v) so increasing batch size to %d",
			currentThroughput, previousThroughput, newSize)
		return newSize, currentThroughput
	}

	newSize := int(currentBatchSize * config.BatchSizeDecreaseFactor)
	log.Infof("latest batch had lower throughput (%v) than previous batch (%v) so decreasing batch size to %d",
		currentThroughput, previousThroughput, newSize)
	return newSize, currentThroughput
}
