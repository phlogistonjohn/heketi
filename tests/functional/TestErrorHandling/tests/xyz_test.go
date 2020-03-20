// +build functional

//
// Copyright (c) 2020 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), as published by the Free Software Foundation,
// or under the Apache License, Version 2.0 <LICENSE-APACHE2 or
// http://www.apache.org/licenses/LICENSE-2.0>.
//
// You may not use this file except in compliance with those terms.
//

package tests

import (
	"os"
	"path"
	"testing"

	"github.com/heketi/tests"

	inj "github.com/heketi/heketi/executors/injectexec"
	_ "github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/testutils"
	"github.com/heketi/heketi/server/config"
)

func TestXyz(t *testing.T) {
	// these "error handling" tests have an unfortunate amount
	// of boilerplate. This is largely needed as the tests are
	// expected to control the heketi server and need to start,
	// stop, and reconfigure it.

	heketiServer := testutils.NewServerCtlFromEnv("..")
	origConf := path.Join(heketiServer.ServerDir, heketiServer.ConfPath)

	baseConf := tests.Tempfile()
	defer os.Remove(baseConf)
	UpdateConfig(origConf, baseConf, func(c *config.Config) {
		// we want the background cleaner disabled for all
		// of the sub-tests we'll be running as we are testing
		// on demand cleaning and want predictable behavior.
		c.GlusterFS.DisableBackgroundCleaner = true
	})

	heketiServer.ConfPath = tests.Tempfile()
	defer os.Remove(heketiServer.ConfPath)
	CopyFile(baseConf, heketiServer.ConfPath)

	defer func() {
		CopyFile(baseConf, heketiServer.ConfPath)
		testutils.ServerRestarted(t, heketiServer)
		testCluster.Teardown(t)
		testutils.ServerStopped(t, heketiServer)
	}()

	resetConfFile := func() {
		CopyFile(baseConf, heketiServer.ConfPath)
	}

	testutils.ServerStarted(t, heketiServer)
	heketiServer.KeepDB = true
	testCluster.Setup(t, 3, 2)

	// do any additional common test setup stuff here

	t.Run("happyPath", func(t *testing.T) {
		// might as well do a simple happy path test before
		// doing more complex error condition tests
	})

	t.Run("errorCond1", func(t *testing.T) {
		resetConfFile()
		UpdateConfig(origConf, heketiServer.ConfPath, func(c *config.Config) {
			c.GlusterFS.Executor = "inject/ssh"
			// c.GlusterFS.InjectConfig.CmdInjection.CmdHooks intercept
			// the command before it is run on a host.
			c.GlusterFS.InjectConfig.CmdInjection.CmdHooks = inj.CmdHooks{
				inj.CmdHook{
					Cmd: ".*I AM A COMMAND REGEX.*",
					Reaction: inj.Reaction{
						Err: "something that shows up in the logs!",
					},
				},
			}
			// c.GlusterFS.InjectConfig.CmdInjection.ResultHooks
			// intercept the command and the result after it is run
			// on a host.
		})
		testutils.ServerRestarted(t, heketiServer)

		// now do any testing of the injected error

		// any additional assertions, etc.
	})

	// ... add as many more t.Run for the cases you need to test
}
