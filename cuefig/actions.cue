import "github.com/emillamm/cuefig/ghactions/workflows"

_wf: workflows & {
	#steps: cue: #Registries: ["github.com/emillamm/cuefig=ghcr.io/emillamm"]
}

actions: publish: _wf.go.#PublishLibraryPublic
actions: test:    _wf.go.#TestPublic
