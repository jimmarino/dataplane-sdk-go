package main

import (
	"github.com/metaform/dataplane-sdk-go/examples/streaming"
	"github.com/metaform/dataplane-sdk-go/examples/streaming-pull-dataplane/launcher"
)

func main() {
	launcher.LaunchServices()

	streaming.TerminateScenario()
}
