
# Advanced Volume Expansion Strategies

## Summary

Currently, heketi supports expanding (file) volumes by increasing the
distribution of gluster volumes. A volume that has three bricks in replica-3
may become a volume containing six bricks, two sets of replica-3.
While this approach to expansion generally works it does come with some
drawbacks. These include:
* The requirement to rebalance volumes after expansion
* Volumes that contain a small number of large files [1]
* Handling of very full bricks
* A general desire to keep volmumes "compact" for simpler managment and
  debugging

Thus a concept of expansions strategies is being proposed.
An expansion strategy is the method that heketi will use when a volume
expansion is requested. Currently the expansion strategies include
the existing "distribute" strategy and the new "grow-brick" strategy.

The "grow-brick" strategy is supported when the free size of the devcies
that contain all bricks in the volume is greater than the requested
expansion size. Intially, the implemenation may
choose to disable the grow brick strategy when the volume has a distribute
count > 1.

Expansion strategies may be chained together such that if the system
detects that one strategy will fail, the next strategy in the chain
may be attempted.


## Interface


## TODO


[1]: It's expected these bricks on general-use volumes do not have sharding
enabled
