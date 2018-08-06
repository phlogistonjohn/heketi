#!/usr/bin/env python

import argparse
import fnmatch
import logging
import os
import subprocess
import sys
import time

try:
    import configparser
except ImportError:
    import ConfigParser as configparser


log = logging.getLogger("funrun")


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


class TestEnv(object):
    def __init__(self, suite):
        self.suite = suite

    def setup(self):
        raise NotImplementedError()

    def teardown(self):
        raise NotImplementedError()

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, ecls, err, tb=None):
        self.teardown()


class EmptyEnv(TestEnv):
    def setup(self):
        pass

    def teardown(self):
        pass


class VagrantEnv(TestEnv):
    def __init__(self, suite):
        self.vagrant_dir = suite.vagrant_dir

    def setup(self):
        log.info("Starting vagrant environment")
        cmd = ['vagrant', 'up', '--no-provision']
        kwargs = dict(cwd=self.vagrant_dir)
        sh(*cmd, **kwargs)

    def teardown(self):
        log.info("Stopping vagrant environment")
        try:
            sh('vagrant', 'destroy', '-f', cwd=self.vagrant_dir)
        except Fail as err:
            log.warning("unable to fully teardown vagrant environment")


def _go_test_runner(packagedir):
    cmd = ['go', 'test', '-timeout=2h', '-tags', 'functional', '-v']
    #opts = dict(cwd=packagedir)
    def runtests():
        curdir = os.path.abspath('.')
        os.chdir(packagedir)
        try:
            sh(*cmd)
        finally:
            os.chdir(curdir)
    return runtests


class TestGroup(object):
    def __init__(self, suite, name):
        self.suite = suite
        self.name = name

    @classmethod
    def from_config(cls, suite, name, cfg):
        g = cls(suite, name)
        ttype = cfg.get('type', 'gotest')
        if ttype == 'gotest':
            g.runner = _go_test_runner(
                packagedir=os.path.join(suite.path, cfg['package']))
        elif ttype == 'script':
            g.runner = _script_test_runner(
                path=os.path.join(suite.path, cfg['path']))
        else:
            raise Fail("invalid test group type: {}".format(ttype))
        return g

    def run(self):
        log.info("Running test group: %s", self.name)
        runtests = self.runner()
        try:
            runtests()
            return True
        except Fail:
            log.error("test group failed: {}".format(self.name))
            return False



class TestSuite(object):
    def __init__(self, cli, name):
        self.name = name
        self.path = os.path.join(cli.func_tests, name)
        self.vagrant_dir = os.path.join(self.path, 'xxxvagrant')
        self._has_vagrant = os.path.isdir(self.vagrant_dir)

    def environment(self):
        if self._has_vagrant:
            return VagrantEnv(self)
        return EmptyEnv(self)

    def groups(self):
        try:
            return self._config_groups()
        except Fail:
            return self._auto_groups()

    def _config_groups(self):
        tests_cfg = os.path.join(self.path, 'suite.ini')
        try:
            with open(tests_cfg) as fh:
                cfg = configparser.SafeConfigParser()
                cfg.readfp(fh)
        except (IOError, OSError):
            raise Fail("no config file in suite {}".format(self.name))
        groups = cfg.get("suite", "groups").split()
        return [TestGroup.from_config(self, name, dict(cfg.items(name)))
                for name in groups]

    def _auto_groups(self):
        tests_dir = os.path.join(self.path, 'tests')
        if os.path.isdir(tests_dir):
            return [TestGroup.from_config(self, 'tests', {})]
        return []

    def __str__(self):
        return self.name


def sh(*args, **kwargs):
    try:
        subprocess.check_call(args, **kwargs)
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
        return subprocess.check_output(args).decode('utf8')
    except subprocess.CalledProcessError as e:
        raise Fail('command failed: {}, exit: {}'.format(args, e.returncode))


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
    suites = fnmatch.filter(SUITES, cli.test_suite)
    return [TestSuite(cli, name) for name in suites]


def get_groups(cli, suite_groups):
    print ("XXX", [g.name for g in suite_groups])
    x = [g for g in suite_groups if fnmatch.fnmatch(g.name, cli.test_group)]
    print ("TTT", x)
    return x


def process_results(results, start_time):
    print ("XXX", results, start_time)


def parse_cli():
    curdir = os.path.abspath('.')
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]))

    parser = argparse.ArgumentParser()
    parser.set_defaults(curdir=curdir, script_dir=script_dir)
    parser.add_argument('--func-tests', default=script_dir)
    parser.add_argument("--test-suite", "-s", default='*')
    parser.add_argument("--test-group", "-g", default='*')

    return parser.parse_args()


def main():
    cli = parse_cli()
    logging.basicConfig(
        level=logging.DEBUG)
    print (cli)
    try:
        check_prereqs()
        # See https://bugzilla.redhat.com/show_bug.cgi?id=1327740
        #_sudo setenforce 0
        suites = get_suites(cli)
        for suite in suites:
            suite.environment().teardown()
        start_time = time.time()
        results = []
        log.debug("Starting functional tests")
        for suite in suites:
            log.info("Running suite: %s", suite)
            with suite.environment():
                for group in get_groups(cli, suite.groups()):
                    print ("NNN", group)
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
