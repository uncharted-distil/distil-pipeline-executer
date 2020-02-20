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
	"fmt"
	"os/exec"
	"path"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"

	"github.com/uncharted-distil/distil-pipeline-executer/env"
)

// Produce produces predictions using the specified model and input data.
func Produce(pipelineID string, schemaFile string, predictionsID string) error {
	// run the produce command
	log.Infof("running produce command using shell")
	predictionOutput := path.Join(env.ResolvePredictionPath(predictionsID), "predictions.csv")
	cmd := exec.Command("python3", "runtime", "-v teststatic", "produce",
		fmt.Sprintf("-t %s", schemaFile),
		fmt.Sprintf("-f %s", env.ResolvePipelineD3MPath(pipelineID)),
		fmt.Sprintf("-o %s", predictionOutput))

	err := cmd.Run()
	if err != nil {
		return errors.Wrap(err, "unable to run produce command")
	}

	return nil
}