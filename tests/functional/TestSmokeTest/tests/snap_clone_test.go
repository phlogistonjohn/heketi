// +build functional

//
// Copyright (c) 2018 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//

package functional

import (
	"fmt"
	"testing"

	"github.com/heketi/heketi/pkg/glusterfs/api"
	_"github.com/heketi/heketi/pkg/utils/ssh"
	"github.com/heketi/tests"
)

func TestCloneVolume(t *testing.T) {
	setupCluster(t, 4, 8)
	defer teardownCluster(t)

	volReq := &api.VolumeCreateRequest{}
	volReq.Size = 10
	volReq.Durability.Type = api.DurabilityReplicate
	volReq.Durability.Replicate.Replica = 3

	r, err := heketi.VolumeCreate(volReq)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
fmt.Println("gggg", r.Name)

	cloneReq := &api.VolumeCloneRequest{}
	r2, err := heketi.VolumeClone(r.Id, cloneReq)
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
fmt.Println("gggg", r2.Name)
}
