#!/bin/bash

set -e

git=git

case "$1" in
    -h|--help|help|"")
        echo "$0 MAJOR_VER RELEASE_VER"
        echo "   example: $0 7 v7.0.0"
        exit 0
    ;;
esac

major_ver="$1"
release_ver="$2"

if [[ "$major_ver" = "" ]]; then
    echo "error: major version not given" >&2
    exit 2
fi
if [[ "$release_ver" = "" ]]; then
    echo "error: release version not given" >&2
    exit 2
fi

#TODO: sanity check inputs

rel_branch="release/$major_ver"

if $git rev-parse "$release_ver" &>/dev/null ; then
    echo "error: $release_ver already exists" >&2
    exit 1
fi
if $git rev-parse "$rel_branch" &>/dev/null || $git rev-parse origin/"$rel_branch" &>/dev/null; then
    echo "error: $rel_branch already exists" >&2
    echo " (note: currently, this script can only create major version releases)"
    exit 1
fi


echo '--- Creating tag & branch'
read -p "Press enter to continue."
$git tag -a -m "Release $major_ver" "$release_ver"
$git checkout "$release_ver" -b "$rel_branch"

echo '--- Building release artifacts'
read -p "Press enter to continue."
make release

echo '--- Updating fromsource dockerfile'
read -p "Press enter to continue."
dockerfile=extras/docker/fromsource/Dockerfile
editor=${EDITOR:vi}
sed -i "s,ENV HEKETI_BRANCH=\"master\",ENV HEKETI_BRANCH=\"$rel_branch\"," \
    $dockerfile
read -p "EDITED $dockerfile. Press enter to review in $EDITOR"
$editor "$dockerfile"
git commit -as -m "Update the Dockerfile for release $major_ver" -e

# TODO: automate creating draft release for github

echo "Next Steps:"
echo " * Push changes to heketi repo on github."
echo " * Create release on github repo."
echo "   Go to: https://github.com/heketi/heketi/releases/new"
echo " * Add branch $rel_branch to docker hub."
echo " * Announce release."
echo "See https://github.com/heketi/heketi/wiki/Release for more details."
