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

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"

	"github.com/uncharted-distil/distil-pipeline-executer/env"
)

// Fit trains the specified model using the provided labelled data.
func Fit(pipelineID string, schemaFile string, predictionsID string) error {
	// run the fit command
	log.Infof("running fit command using shell")
	cmd := exec.Command("python3", "runner.py", "runtime", "-v teststatic", "fit",
		fmt.Sprintf("-r %s", env.ResolveProblemPath(pipelineID)),
		fmt.Sprintf("-i %s", schemaFile),
		fmt.Sprintf("-p %s", env.ResolvePipelineJSONPath(pipelineID)),
		fmt.Sprintf("-s %s", env.ResolvePipelineD3MPath(pipelineID)))

	err := cmd.Run()
	if err != nil {
		return errors.Wrap(err, "unable to run fit command")
	}

	return nil
}
