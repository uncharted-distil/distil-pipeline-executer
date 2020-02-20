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

package main

import (
	"net/http"
	"os"
	"syscall"

	"github.com/davecgh/go-spew/spew"
	log "github.com/unchartedsoftware/plog"
	"github.com/zenazn/goji/graceful"
	goji "goji.io/v3"
	"goji.io/v3/pat"

	"github.com/uncharted-distil/distil-pipeline-executer/env"
	"github.com/uncharted-distil/distil-pipeline-executer/routes"
	"github.com/uncharted-distil/distil/api/middleware"
)

var (
	version        = "unset"
	timestamp      = "unset"
	problemPath    = ""
	datasetDocPath = ""
)

func registerRoute(mux *goji.Mux, pattern string, handler func(http.ResponseWriter, *http.Request)) {
	log.Infof("Registering GET route %s", pattern)
	mux.HandleFunc(pat.Get(pattern), handler)
}

func registerRoutePost(mux *goji.Mux, pattern string, handler func(http.ResponseWriter, *http.Request)) {
	log.Infof("Registering POST route %s", pattern)
	mux.HandleFunc(pat.Post(pattern), handler)
}

func main() {
	log.Infof("version: %s built: %s", version, timestamp)

	// load config from env
	config, err := env.LoadConfig()
	if err != nil {
		log.Errorf("%+v", err)
		os.Exit(1)
	}
	log.Infof("%+v", spew.Sdump(config))
	env.Initialize(config)

	// register routes
	mux := goji.NewMux()
	mux.Use(middleware.Log)
	mux.Use(middleware.Gzip)

	routes.SetVerboseError(config.VerboseError)
	// GET
	registerRoute(mux, "/distil/pipelines", routes.PipelinesHandler(config))
	registerRoute(mux, "/distil/config", routes.ConfigHandler(config, version, timestamp))

	// POST
	//registerRoutePost(mux, "/distil/fit/:pipeline-id", routes.FitHandler(pgDataStorageCtor, esMetadataStorageCtor))
	//registerRoutePost(mux, "/distil/produce/:pipeline-id", routes.ProduceHandler(pgDataStorageCtor, esMetadataStorageCtor))

	// static
	registerRoute(mux, "/*", routes.FileHandler("./dist"))

	// catch kill signals for graceful shutdown
	graceful.AddSignal(syscall.SIGINT, syscall.SIGTERM)

	// kick off the server listen loop
	log.Infof("Listening on port %s", config.AppPort)
	err = graceful.ListenAndServe(":"+config.AppPort, mux)
	if err != nil {
		log.Errorf("%+v", err)
		os.Exit(1)
	}

	// wait until server gracefully exits
	graceful.Wait()
}
