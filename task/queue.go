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

// Queue queues rows from datasets.
type Queue struct {
	datasets map[string][][]string
}

// NewQueue creates a new queue.
func NewQueue() *Queue {
	return &Queue{
		datasets: make(map[string][][]string),
	}
}

// AddDataset adds a dataset to the queue.
func (q *Queue) AddDataset(dataset string) {
	q.datasets[dataset] = make([][]string, 0)
}

// GetLength returns the count of entries in the queue for the specified dataset.
func (q *Queue) GetLength(dataset string) int {
	return len(q.datasets[dataset])
}

// AddEntry adds a row to the dataset queue.
func (q *Queue) AddEntry(dataset string, row []string) {
	q.datasets[dataset] = append(q.datasets[dataset], row)
}

// RemoveEntry removes the first row from the dataset queue.
func (q *Queue) RemoveEntry(dataset string) []string {
	entries := q.RemoveEntries(dataset, 1)
	if len(entries) < 1 {
		return nil
	}
	return entries[1]
}

// RemoveEntries removes the first n rows from the dataset queue.
func (q *Queue) RemoveEntries(dataset string, count int) [][]string {
	datasetData := q.datasets[dataset]
	if len(datasetData) == 0 {
		return nil
	} else if len(datasetData) < count {
		count = len(datasetData)
	}

	entries := datasetData[:count]
	datasetData = datasetData[count:]
	q.datasets[dataset] = datasetData

	return entries
}
