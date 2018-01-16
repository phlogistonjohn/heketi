//
// Copyright (c) 2018 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//


package glusterfs

import (
	"time"

	"github.com/boltdb/bolt"
)

const (
	BOLTDB_BUCKET_CLUSTER          = "CLUSTER"
	BOLTDB_BUCKET_NODE             = "NODE"
	BOLTDB_BUCKET_VOLUME           = "VOLUME"
	BOLTDB_BUCKET_DEVICE           = "DEVICE"
	BOLTDB_BUCKET_BRICK            = "BRICK"
	BOLTDB_BUCKET_BLOCKVOLUME      = "BLOCKVOLUME"
	BOLTDB_BUCKET_DBATTRIBUTE      = "DBATTRIBUTE"
	DB_CLUSTER_HAS_FILE_BLOCK_FLAG = "DB_CLUSTER_HAS_FILE_BLOCK_FLAG"
	// default values
	DB_FILE_NAME = "heketi.db"
)


func DbOpen(filename string) (*bolt.DB, bool, error) {
	var db *bolt.DB
	var err error
	db, err = bolt.Open(filename, 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		logger.LogError("Unable to open database: %v. Retrying using read only mode", err)

		// Try opening as read-only
		db, err = bolt.Open(filename, 0666, &bolt.Options{
			ReadOnly: true,
		})
		if err != nil {
			return db, false, err
		}
		return db, true, nil
	} else {
		err = DbInitialize(db)
		if err != nil {
			return db, false, err
		}
	}
	return db, false, nil
}

func DbInitialize(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		// Create Cluster Bucket
		_, err := tx.CreateBucketIfNotExists([]byte(BOLTDB_BUCKET_CLUSTER))
		if err != nil {
			logger.LogError("Unable to create cluster bucket in DB")
			return err
		}

		// Create Node Bucket
		_, err = tx.CreateBucketIfNotExists([]byte(BOLTDB_BUCKET_NODE))
		if err != nil {
			logger.LogError("Unable to create node bucket in DB")
			return err
		}

		// Create Volume Bucket
		_, err = tx.CreateBucketIfNotExists([]byte(BOLTDB_BUCKET_VOLUME))
		if err != nil {
			logger.LogError("Unable to create volume bucket in DB")
			return err
		}

		// Create Device Bucket
		_, err = tx.CreateBucketIfNotExists([]byte(BOLTDB_BUCKET_DEVICE))
		if err != nil {
			logger.LogError("Unable to create device bucket in DB")
			return err
		}

		// Create Brick Bucket
		_, err = tx.CreateBucketIfNotExists([]byte(BOLTDB_BUCKET_BRICK))
		if err != nil {
			logger.LogError("Unable to create brick bucket in DB")
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte(BOLTDB_BUCKET_BLOCKVOLUME))
		if err != nil {
			logger.LogError("Unable to create blockvolume bucket in DB")
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte(BOLTDB_BUCKET_DBATTRIBUTE))
		if err != nil {
			logger.LogError("Unable to create dbattribute bucket in DB")
			return err
		}

		// Handle Upgrade Changes
		err = DbUpgrade(tx)
		if err != nil {
			logger.LogError("Unable to Upgrade Changes")
			return err
		}

		return nil

	})
}


// DbUpgrade Path to update all the values for new API entries
func DbUpgrade(tx *bolt.Tx) error {

	err := ClusterEntryUpgrade(tx)
	if err != nil {
		logger.LogError("Failed to upgrade db for cluster entries")
		return err
	}

	err = NodeEntryUpgrade(tx)
	if err != nil {
		logger.LogError("Failed to upgrade db for node entries")
		return err
	}

	err = VolumeEntryUpgrade(tx)
	if err != nil {
		logger.LogError("Failed to upgrade db for volume entries")
		return err
	}

	err = DeviceEntryUpgrade(tx)
	if err != nil {
		logger.LogError("Failed to upgrade db for device entries")
		return err
	}

	err = BrickEntryUpgrade(tx)
	if err != nil {
		logger.LogError("Failed to upgrade db for brick entries: %v", err)
		return err
	}

	return nil
}
