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
	"math/rand"

	"github.com/boltdb/bolt"

	"github.com/heketi/heketi/pkg/glusterfs/api"
)

func ChurnOMatic(app *App, count int) error {
	for i := 0; i < count; i++ {
		logger.Info("Churn pass: %+v", i)
		err := churnOnce(app, i)
		if err != nil {
			return err
		}
	}
	return nil
}

func churnOnce(app *App, cpass int) error {
	n := rand.Intn(5)
	logger.Info("N=%v", n)
	switch n {
	case 0, 1, 2, 3:
		return churnItem(app, cpass, n)
	case 4:
		logger.Info("Churn - Doing a run!")
		c := 10 + rand.Intn(41)
		x := rand.Intn(4)
		for i := 0; i < c; i++ {
			err := churnItem(app, cpass, x)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func churnItem(app *App, cpass, n int) error {
	switch n {
	case 0:
		logger.Info("Churn - Create Volume")
		return churnCreateVolume(app)
	case 1:
		logger.Info("Churn - Delete Volume")
		return churnDeleteVolume(app)
	case 2:
		logger.Info("Churn - Create Block Volume")
		return churnCreateBlockVolume(app)
	case 3:
		logger.Info("Churn - Delete Block Volume")
		return churnDeleteBlockVolume(app)
	}
	return nil
}

func churnCreateVolume(app *App) error {
	req := &api.VolumeCreateRequest{}
	req.Size = 1 + rand.Intn(10) // TODO wider range
	req.Durability.Type = api.DurabilityReplicate
	req.Durability.Replicate.Replica = 3

	vol := NewVolumeEntryFromRequest(req)
	vc := NewVolumeCreateOperation(vol, app.db)
	err := RunOperation(vc, app.executor)
	if err != nil {
		logger.Err(err)
	}
	return nil
}

func churnDeleteVolume(app *App) error {
	var vol *VolumeEntry
	noVols := fmt.Errorf("no vols")
	hostingVol := fmt.Errorf("hosting vol occupado")
	err := app.db.View(func(tx *bolt.Tx) error {
		vl, e := VolumeList(tx)
		if e != nil {
			return e
		}
		if len(vl) == 0 {
			return noVols
		}
		i := rand.Intn(len(vl))
		vol, e = NewVolumeEntryFromId(tx, vl[i])
		if e != nil {
			return e
		}
		if len(vol.Info.BlockInfo.BlockVolumes) > 0 {
			return hostingVol
		}
		return e
	})
	if err == noVols || err == hostingVol {
		logger.Info("no can delete")
		return nil
	}
	if err != nil {
		logger.Err(err)
		return err
	}

	vd := NewVolumeDeleteOperation(vol, app.db)
	err = RunOperation(vd, app.executor)
	if err != nil {
		logger.Err(err)
	}
	return nil
}

func churnCreateBlockVolume(app *App) error {
	req := &api.BlockVolumeCreateRequest{}
	req.Size = 4 + rand.Intn(17) // TODO wider range

	vol := NewBlockVolumeEntryFromRequest(req)
	vc := NewBlockVolumeCreateOperation(vol, app.db)
	err := RunOperation(vc, app.executor)
	if err != nil {
		logger.Err(err)
	}
	return nil
}

func churnDeleteBlockVolume(app *App) error {
	var vol *BlockVolumeEntry
	noVols := fmt.Errorf("no vols")
	err := app.db.View(func(tx *bolt.Tx) error {
		vl, e := BlockVolumeList(tx)
		if e != nil {
			return e
		}
		if len(vl) == 0 {
			return noVols
		}
		i := rand.Intn(len(vl))
		vol, e = NewBlockVolumeEntryFromId(tx, vl[i])
		return e
	})
	if err == noVols {
		logger.Info("No volumes to delete")
		return nil
	}
	if err != nil {
		logger.Err(err)
		return err
	}

	bdel := NewBlockVolumeDeleteOperation(vol, app.db)
	err = RunOperation(bdel, app.executor)
	if err != nil {
		logger.Err(err)
	}
	return nil
}
