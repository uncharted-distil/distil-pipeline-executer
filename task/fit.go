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
	"fmt"
	"os/exec"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"

	"github.com/uncharted-distil/distil-pipeline-executer/env"
)

// Fit trains the specified model using the provided labelled data.
func Fit(pipelineID string, schemaFile string, predictionsID string, config *env.Config) error {
	// run the fit command
	log.Infof("running fit command using shell")
	commandLine := fmt.Sprintf("python3 runner.py runtime -v %s fit -r %s -i %s -p %s -s %s",
		config.D3MStaticDir, env.ResolveProblemPath(pipelineID), schemaFile,
		env.ResolvePipelineJSONPath(pipelineID), env.ResolvePipelineD3MPath(pipelineID))
	cmd := exec.Command("/bin/sh", "-c", commandLine)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	log.Infof("out: %s", stdout.String())
	if err != nil {
		log.Errorf("err: %s", stderr.String())
		return errors.Wrap(err, "unable to run fit command")
	}

	return nil
}
