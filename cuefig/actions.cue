import "github.com/emillamm/cuefig/ghactions/workflows"

_wf: workflows & {
	#steps: cue: #Registries: ["github.com/emillamm/cuefig=ghcr.io/emillamm"]
}

actions: publish: _wf.go.#PublishLibrary
actions: test:    _wf.go.#Test
