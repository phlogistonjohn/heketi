//
// Copyright (c) 2019 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//

package glusterfs

import (
	"fmt"

	"github.com/heketi/heketi/executors/cmdexec"
)

// ExecWithApp runs the commands specified on the host
// within the app's executor.
func ExecWithApp(app *App, target string, commands []string, timeout int) error {
	r, ok := app.executor.(*cmdexec.CmdExecutor)
	if !ok {
		return fmt.Errorf("Invalid executor")
	}
	_, err := r.RemoteExecutor.ExecCommands(
		target, commands, timeout)
	return err
}
