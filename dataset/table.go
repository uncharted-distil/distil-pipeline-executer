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

package dataset

import (
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/uncharted-distil/distil-pipeline-executer/model"
)

// Table represents a basic table dataset.
type Table struct {
	ID   string `json:"id"`
	Rows []Row  `json:"rows"`
}

// Row is a row of table data, tagged with an id.
type Row struct {
	ID   string            `json:"id"`
	Data map[string]string `json:"data"`
}

// NewTableDataset creates a new table dataset from raw byte data, assuming json
func NewTableDataset(rawData []byte) (*Table, error) {
	table := &Table{}
	err := json.Unmarshal(rawData, table)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse json")
	}

	return table, nil
}

// CreateDataset processes the table data structure into a dataset that can
// be used in the D3M ecosystem.
func (t *Table) CreateDataset(rootPath string) (*model.Dataset, error) {
	// get the list of fields on the first pass
	fieldMap := map[string]int{"d3mIndex": 0}
	for _, row := range t.Rows {
		for f := range row.Data {
			if fieldMap[f] == 0 {
				fieldMap[f] = len(fieldMap)
			}
		}
	}

	// create the learning data using the field map
	learningData := make([][]string, 0)
	for _, row := range t.Rows {
		entry := make([]string, len(fieldMap))
		entry[0] = row.ID
		for f, d := range row.Data {
			entry[fieldMap[f]] = d
		}
		learningData = append(learningData, entry)
	}

	// get the columns
	columns := make([]string, len(fieldMap))
	for f, c := range fieldMap {
		columns[c] = f
	}

	return &model.Dataset{
		ID:        t.ID,
		Variables: columns,
		Data:      learningData,
	}, nil
}

// GetPredictionsID returns the prediction set id.
func (t *Table) GetPredictionsID() string {
	return t.ID
}
