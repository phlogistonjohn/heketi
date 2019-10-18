//
// Copyright (c) 2019 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//

/*
YEAH, this is silly:
EVIL=damaged.db  ./heketi offline churn  --config heketi.json  --iterations 1
*/

package glusterfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"

	"github.com/boltdb/bolt"

	"github.com/heketi/heketi/pkg/glusterfs/api"
)

type Extract interface {
	Reset() error
	Key() []byte
	BackOffset() int
	Unmarshal([]byte) error
	Report() error
}

func ChurnOMatic(app *App, trashFile string) error {
	logger.Info("Opening: %s", trashFile)
	x, err := ioutil.ReadFile(trashFile)
	if err != nil {
		return err
	}

	var dump Db
	dump.Clusters = make(map[string]ClusterEntry, 0)
	dump.Volumes = make(map[string]VolumeEntry, 0)
	dump.BlockVolumes = make(map[string]BlockVolumeEntry, 0)
	dump.Bricks = make(map[string]BrickEntry, 0)
	dump.Nodes = make(map[string]NodeEntry, 0)
	dump.Devices = make(map[string]DeviceEntry, 0)
	dump.DbAttributes = make(map[string]DbAttributeEntry, 0)
	dump.PendingOperations = make(map[string]PendingOperationEntry, 0)

	volExtractor := newVolumeExtractor(&dump)
	fmt.Printf("Volumes: %v\n", exhume(x, volExtractor))

	bvExtractor := newBlockVolumeExtractor(&dump)
	fmt.Printf("Block Volumes: %v\n", exhume(x, bvExtractor))

	bricksExtractor := newBrickExtractor(&dump)
	fmt.Printf("Bricks: %v\n", exhume(x, bricksExtractor))

	nodesExtractor := newNodeExtractor(&dump)
	fmt.Printf("Nodes: %v\n", exhume(x, nodesExtractor))

	devicesExtractor := newDeviceExtractor(&dump)
	fmt.Printf("Devices: %v\n", exhume(x, devicesExtractor))

	clusterExtractor := newClusterExtractor(&dump)
	fmt.Printf("Clusters: %v\n", exhume(x, clusterExtractor))

	fmt.Printf("\n")
	fp := os.Stdout
	enc := json.NewEncoder(fp)
	enc.SetIndent("", "    ")
	if err := enc.Encode(dump); err != nil {
		return fmt.Errorf("Could not encode dump as JSON: %v", err.Error())
	}
	return nil
}

func exhume(x []byte, extractor Extract) error {
	for pos := 0; pos < len(x); {
		err := extractor.Reset()
		if err != nil {
			return err
		}
		fmt.Printf("pos=%v\n", pos)
		x1 := x[pos:]
		p2 := bytes.Index(x1, extractor.Key())
		fmt.Printf("(found %v)", p2)
		if p2 == -1 {
			return fmt.Errorf("not found")
		}
		pos = pos + p2 - extractor.BackOffset()
		fmt.Printf("FOUND pos=%v\n", pos)

		var e int
		for e = pos + 64; e < (pos + 4096); e++ {
			x2 := x[pos:e]
			//fmt.Printf("X2: %v, %v, %+v\n", pos, e, x2)
			err = extractor.Unmarshal(x2)
			if err == nil {
				break
			}
		}
		pos = e + 1
		//fmt.Printf("SET pos=%v\n", pos)
		if err := extractor.Report(); err != nil {
			return err
		}
	}

	return nil
}

type volumeExtractor struct {
	v  *VolumeEntry
	db *Db
}

func newVolumeExtractor(db *Db) *volumeExtractor {
	return &volumeExtractor{db: db}
}

func (ve *volumeExtractor) Key() []byte {
	return []byte("\x0bVolumeEntry")
}

func (ve *volumeExtractor) BackOffset() int {
	return 6
}

func (ve *volumeExtractor) Reset() error {
	ve.v = nil
	return nil
}

func (ve *volumeExtractor) Unmarshal(buf []byte) error {
	ve.v = NewVolumeEntry()
	return ve.v.Unmarshal(buf)
}

func (ve *volumeExtractor) Report() error {
	if ve.v == nil {
		return fmt.Errorf("No volume found")
	}
	if ve.v.Info.Id == "" {
		return fmt.Errorf("Incomplete volume")
	}
	fmt.Printf("V: %s %s\n", ve.v.Info.Id, ve.v.Info.Name)
	ve.db.Volumes[ve.v.Info.Id] = *ve.v
	return nil
}

//---
type blockVolumeExtractor struct {
	v  *BlockVolumeEntry
	db *Db
}

func newBlockVolumeExtractor(db *Db) *blockVolumeExtractor {
	return &blockVolumeExtractor{db: db}
}

func (ve *blockVolumeExtractor) Key() []byte {
	return []byte("BlockVolumeEntry")
}

func (ve *blockVolumeExtractor) BackOffset() int {
	return 7
}

func (ve *blockVolumeExtractor) Reset() error {
	ve.v = nil
	return nil
}

func (ve *blockVolumeExtractor) Unmarshal(buf []byte) error {
	ve.v = NewBlockVolumeEntry()
	return ve.v.Unmarshal(buf)
}

func (ve *blockVolumeExtractor) Report() error {
	if ve.v == nil {
		return fmt.Errorf("No volume found")
	}
	if ve.v.Info.Id == "" {
		return fmt.Errorf("Incomplete volume")
	}
	fmt.Printf("V: %s %s\n", ve.v.Info.Id, ve.v.Info.Name)
	ve.db.BlockVolumes[ve.v.Info.Id] = *ve.v
	return nil
}

//---
type brickExtractor struct {
	b  *BrickEntry
	db *Db
}

func newBrickExtractor(db *Db) *brickExtractor {
	return &brickExtractor{db: db}
}

func (ve *brickExtractor) Key() []byte {
	return []byte("BrickEntry")
}

func (ve *brickExtractor) BackOffset() int {
	return 7
}

func (ve *brickExtractor) Reset() error {
	ve.b = nil
	return nil
}

func (ve *brickExtractor) Unmarshal(buf []byte) error {
	ve.b = NewBrickEntry(0, 0, 0, "", "", 0, "")
	return ve.b.Unmarshal(buf)
}

func (ve *brickExtractor) Report() error {
	if ve.b == nil {
		return fmt.Errorf("No brick found")
	}
	if ve.b.Info.Id == "" {
		return fmt.Errorf("Incomplete brick")
	}
	fmt.Printf("B: %s %s\n", ve.b.Info.Id, ve.b.Info.Path)
	ve.db.Bricks[ve.b.Info.Id] = *ve.b
	return nil
}

//---
type deviceExtractor struct {
	device *DeviceEntry
	db     *Db
}

func newDeviceExtractor(db *Db) *deviceExtractor {
	return &deviceExtractor{db: db}
}

func (ve *deviceExtractor) Key() []byte {
	return []byte("DeviceEntry")
}

func (ve *deviceExtractor) BackOffset() int {
	return 7
}

func (ve *deviceExtractor) Reset() error {
	ve.device = nil
	return nil
}

func (ve *deviceExtractor) Unmarshal(buf []byte) error {
	ve.device = NewDeviceEntry()
	return ve.device.Unmarshal(buf)
}

func (ve *deviceExtractor) Report() error {
	if ve.device == nil {
		return fmt.Errorf("No device found")
	}
	if ve.device.Info.Id == "" {
		return fmt.Errorf("Incomplete device")
	}
	fmt.Printf("D: %s %s\n", ve.device.Info.Id)
	ve.db.Devices[ve.device.Info.Id] = *ve.device
	return nil
}

//---
type nodeExtractor struct {
	node *NodeEntry
	db   *Db
}

func newNodeExtractor(db *Db) *nodeExtractor {
	return &nodeExtractor{db: db}
}

func (ve *nodeExtractor) Key() []byte {
	return []byte("NodeEntry")
}

func (ve *nodeExtractor) BackOffset() int {
	return 7
}

func (ve *nodeExtractor) Reset() error {
	ve.node = nil
	return nil
}

func (ve *nodeExtractor) Unmarshal(buf []byte) error {
	ve.node = NewNodeEntry()
	return ve.node.Unmarshal(buf)
}

func (ve *nodeExtractor) Report() error {
	if ve.node == nil {
		return fmt.Errorf("No node found")
	}
	if ve.node.Info.Id == "" {
		return fmt.Errorf("Incomplete node")
	}
	fmt.Printf("D: %s %s\n", ve.node.Info.Id)
	ve.db.Nodes[ve.node.Info.Id] = *ve.node
	return nil
}

//---
type clusterExtractor struct {
	cluster *ClusterEntry
	db      *Db
}

func newClusterExtractor(db *Db) *clusterExtractor {
	return &clusterExtractor{db: db}
}

func (ve *clusterExtractor) Key() []byte {
	return []byte("ClusterEntry")
}

func (ve *clusterExtractor) BackOffset() int {
	return 7
}

func (ve *clusterExtractor) Reset() error {
	ve.cluster = nil
	return nil
}

func (ve *clusterExtractor) Unmarshal(buf []byte) error {
	ve.cluster = NewClusterEntry()
	return ve.cluster.Unmarshal(buf)
}

func (ve *clusterExtractor) Report() error {
	if ve.cluster == nil {
		return fmt.Errorf("No cluster found")
	}
	if ve.cluster.Info.Id == "" {
		return fmt.Errorf("Incomplete cluster")
	}
	fmt.Printf("D: %s %s\n", ve.cluster.Info.Id)
	ve.db.Clusters[ve.cluster.Info.Id] = *ve.cluster
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
