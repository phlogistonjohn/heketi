#!/usr/bin/env python

import argparse
import os
import subprocess
import sys


class Fail(Exception):
    pass


SUITES = [
    "TestSelfTest",
    "TestSmokeTest",
    "TestVolumeNotDeletedWhenNodeIsDown",
    "TestVolumeSnapshotBehavior",
    "TestManyBricksVolume",
    "TestUpgrade",
    "TestEnabledTLS",
]

MIN_GO_VERSION = "1.8.3"


def sh(*args):
    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError as e:
        raise Fail('command failed: {}, exit: {}'.format(args, e.returncode))

def cmd_quiet(*args):
    try:
        with open(os.devnull, 'w') as fh:
            subprocess.check_call(args, stdout=fh)
    except subprocess.CalledProcessError as e:
        raise Fail('command failed: {}, exit: {}'.format(args, e.returncode))


def cmd_output(*args):
    try:
        return subprocess.check_output(args)
    except subprocess.CalledProcessError as e:
        raise Fail('command failed: {}, exit: {}'.format(args, e.returncode))


class TestSuite(object):
    def __init__(self, cli, name):
        self.name = name
        self.path = os.path.join(cli.func_tests, name)
        self._has_vagrant = os.path.isdir(os.path.join(self.path, 'vagrant'))

    def environment():
        if self._has_vagrant:
            return VagrantEnv(self)
        return EmptyEnv(self)


class EmptyEnv(object):
    def __init__(self, suite):
        pass

    def teardown(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, ecls, err, tb=None):
        pass


class VagrantEnv(object):
    def __init__(self, suite):
        pass

    def setup(self):
        pass

    def teardown(self):
        pass

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, ecls, err, tb=None):
        self.teardown()
        pass


def check_prereqs():
    try:
        cmd_quiet('go', 'help')
    except Fail:
        raise Fail('"go" tool not found - go is required to run tests')
    try:
        cmd_quiet('glide', '--help')
    except Fail:
        raise Fail('"glide" tool not found - glide is required to build server')
    gversion = cmd_output('go', 'version').split()[2].replace('go', '')
    need_version = tuple(int(v) for v in MIN_GO_VERSION.split('.'))
    go_version = tuple(int(v) for v in gversion.split('.'))
    if need_version > go_version:
        raise Fail('"go" version {} is below required version {}'
                   .format(gversion, MIN_GO_VERSION))
    return


def get_suites(cli):
    return [TestSuite(cli, name) for name in SUITES]


def parse_cli():
    curdir = os.path.abspath('.')
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]))

    parser = argparse.ArgumentParser()
    parser.set_defaults(curdir=curdir, script_dir=script_dir)
    parser.add_argument('--func-tests', default=script_dir)

    return parser.parse_args()



def main():
    cli = parse_cli()
    print cli
    try:
        check_prereqs()
        # See https://bugzilla.redhat.com/show_bug.cgi?id=1327740
        #_sudo setenforce 0
        suites = get_suites(cli)
        for suite in suites:
            suite.environment().teardown()
        start_time = time.time()
        results = []
        for suite in suites:
            with suite.environment():
                for group in suite.groups():
                    results.append(group.run())
        if process_results(results, start_time):
            sys.exit(0)
        else:
            sys.exit(1)
    except Fail as e:
        sys.stderr.write("ERROR: {}\n".format(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
