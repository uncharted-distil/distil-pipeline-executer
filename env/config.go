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
	"sync"

	"github.com/caarlos0/env"
)

var (
	cfg  *Config
	once sync.Once
)

// Config represents the application configuration state loaded from env vars.
type Config struct {
	AppPort       string `env:"PORT" envDefault:"8080"`
	D3MOutputDir  string `env:"D3MOUTPUTDIR" envDefault:"outputs"`
	DatasetDir    string `env:"DATASET_DIR" envDefault:"datasets"`
	PipelineD3M   string `env:"PIPELINE_D3M" envDefault:"pipeline.d3m"`
	PipelineDir   string `env:"PIPELINE_DIR" envDefault:"pipelines"`
	PipelineJSON  string `env:"PIPELINE_JSON" envDefault:"pipeline.json"`
	PredictionDir string `env:"PREDICTION_DIR" envDefault:"predictions"`
	ProblemFile   string `env:"PROBLEM_FILE" envDefault:"problem.json"`
	VerboseError  bool   `env:"VERBOSE_ERROR" envDefault:"false"`
}

// LoadConfig loads the config from the environment if necessary and returns a
// copy.
func LoadConfig() (Config, error) {
	var err error
	once.Do(func() {
		cfg = &Config{}
		err = env.Parse(cfg)
		if err != nil {
			cfg = &Config{}
		}
	})
	return *cfg, err
}
