package client

import (
	"runtime/debug"
)

func GetClientVersion() string {
	defaultVersion := "v1.0.0"

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return defaultVersion
	}

	// If the version number is greater than 1.x, need to modify the path, add v1 tag
	currentModulePath := "github.com/XiaoMi/talos-sdk-golang"

	for _, dep := range info.Deps {
		if dep.Path == currentModulePath {
			if dep.Version != "" {
				return dep.Version
			}
			break
		}
	}

	return defaultVersion
}
