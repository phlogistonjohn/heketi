//
// Copyright (c) 2019 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//

package db

import (
	"os"

	"github.com/boltdb/bolt"
)

// alias bolt types to isolate all db library functions to this
// package

type Options = bolt.Options
type DBHandle = bolt.DB


func Open(path string, mode os.FileMode, opts *Options) (*DBHandle, error) {
	return bolt.Open(path, mode, opts)
}
