#!/bin/bash

h=./heketi

fail() {
    echo "fail!" 1>&2
    exit 1
}

mkdir -p _results
cp heketi.db.fresh heketi.db

for x in {1..25}; do
    k=$((x*10))
    if [[ $k -gt 500 ]] ; then
        k=500
    fi
    $h offline churn --config=heketi.json --iterations=$k &> _results/r$x
    $h db export --dbfile heketi.db --jsonfile _results/j$x || fail
    du -sh heketi.db | tee _results/du_$x
done

