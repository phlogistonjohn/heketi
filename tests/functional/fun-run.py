#!/usr/bin/env python

import argparse
import fnmatch
import logging
import os
import shutil
import subprocess
import sys
import time
import yaml

try:
    from urllib.request import urlopen
except:
    from urllib2 import urlopen


CONFIG_NAME = 'testgroup.yaml'

log = logging.getLogger("funrun")


class Fail(Exception):
    pass


DIRS = [
    "TestSelfTest",
    "TestSmokeTest",
    "TestVolumeNotDeletedWhenNodeIsDown",
    "TestVolumeSnapshotBehavior",
    "TestManyBricksVolume",
    "TestUpgrade",
    "TestEnabledTLS",
]

MIN_GO_VERSION = "1.8.3"


class Filter(object):
    def match(self, value):
        return True


class TestSuite(object):
    pass


class GoTestSuite(TestSuite):
    def __init__(self, package):
        self.package = package

    def __str__(self):
        parent, pn = os.path.split(self.package)
        pdn = os.path.basename(parent)
        return 'Go Test Suite {}/{}'.format(pdn, pn)

    def run(self, *args):
        cmd = ['go', 'test', '-timeout=2h', '-tags', 'functional', '-v']
        env = os.environ.copy()
        # Go is a bit quirky about how it figures out what paths to
        # look in for depencies (vendor dir). Setting PWD explicitly
        # to the new dir seems to help.
        env['PWD'] = self.package
        sh(*cmd, cwd=self.package, env=env)


class TestGroup(object):
    def __init__(self, group_dir):
        self.group_dir = group_dir
        self.name = os.path.basename(group_dir)
        self._defaults()
        self._parse()

    def _defaults(self):
        self._vagrant = os.path.join(self.group_dir, 'vagrant')
        if not os.path.isdir(self._vagrant):
            self._vagrant = None
        self._tests_dir = os.path.join(self.group_dir, 'tests')
        if not os.path.isdir(self._tests_dir):
            self._tests_dir = None

    def _parse(self):
        data = {}
        tg_config = os.path.join(self.group_dir, CONFIG_NAME)
        try:
            with open(tg_config) as fh:
                data = yaml.safe_load(fh)
        except (OSError, IOError):
            pass
        if 'environments' in data:
            pass
        if 'suites' in data:
            pass
        else:
            self._suites = []
            if self._tests_dir:
                self._suites.append(GoTestSuite(self._tests_dir))

    def suites(self, filter=None):
        return self._suites

    def environments(self):
        envs = []
        if self._vagrant:
            envs.append(VagrantEnv(self._vagrant))
        if os.path.isdir(os.path.join(self.group_dir, 'config')):
            envs.append(HeketiEnv(self.group_dir))
        return envs

    def __str__(self):
        return self.name


class TestCollection(object):
    def __init__(self, base_dir):
        self.base_dir = base_dir

    def groups(self, filter=None):
        for d in DIRS:
            group_dir = os.path.join(self.base_dir, d)
            if not os.path.isdir(group_dir):
                continue
            if not filter.match(d):
                continue
            yield TestGroup(group_dir)

    def __str__(self):
        return 'Source {}'.format(self.base_dir)


class EmptyEnv(object):
    name = 'none'

    def setup(self):
        pass

    def teardown(self):
        pass


class VagrantEnv(object):
    def __init__(self, vagrant_dir):
        self.name = 'vagrant'
        self.vagrant_dir = vagrant_dir

    def setup(self):
        log.info("Starting vagrant environment")
        sh('./up.sh', cwd=self.vagrant_dir)

    def teardown(self):
        log.info("Stopping vagrant environment")
        try:
            sh('vagrant', 'destroy', '-f', cwd=self.vagrant_dir)
        except Fail as err:
            log.warning("unable to fully teardown vagrant environment")

    def __str__(self):
        return 'Vagrant'


class HeketiEnv(object):
    def __init__(self, run_dir):
        self.name = 'heketi'
        self.run_dir = run_dir
        self._proc = None

    def setup(self):
        d = os.path.dirname
        heketi_dir = d(d(d(self.run_dir)))
        env = os.environ.copy()
        env['PWD'] = heketi_dir
        sh('go', 'build', cwd=heketi_dir, env=env)
        shutil.copy(
            os.path.join(heketi_dir, 'heketi'),
            os.path.join(self.run_dir, 'heketi-server'))

        dbpath = os.path.join(self.run_dir, 'heketi.db')
        try:
            os.unlink(dbpath)
        except OSError:
            pass
        log.info("RUN DIR %s", self.run_dir)
        self._proc = subprocess.Popen(
            ['./heketi-server', '--config=config/heketi.json'],
            cwd=self.run_dir)
        for _ in range(0, 60):
            try:
                r = urlopen('http://localhost:8080/hello')
                if r.status != 200:
                    raise ValueError('not alive')
                found = True
            except Exception:
                log.debug("unable to connect to server")
                time.sleep(1)
        if not found:
            raise Fail("unable to connect to heketi server")

    def teardown(self):
        self._proc.terminate()

    def __str__(self):
        return 'Heketi Server'


def sh(*args, **kwargs):
    log.debug("running: %s", args)
    try:
        subprocess.check_call(args, **kwargs)
    except subprocess.CalledProcessError as e:
        raise Fail('command failed: {}, exit: {}'.format(args, e.returncode))


def cmd_quiet(*args):
    log.debug("running: %s", args)
    try:
        with open(os.devnull, 'w') as fh:
            subprocess.check_call(args, stdout=fh)
    except subprocess.CalledProcessError as e:
        raise Fail('command failed: {}, exit: {}'.format(args, e.returncode))


def cmd_output(*args):
    log.debug("running: %s", args)
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
        raise Fail('"glide" tool not found: glide is required to build server')
    gversion = cmd_output('go', 'version').split()[2].replace('go', '')
    need_version = tuple(int(v) for v in MIN_GO_VERSION.split('.'))
    go_version = tuple(int(v) for v in gversion.split('.'))
    if need_version > go_version:
        raise Fail('"go" version {} is below required version {}'
                   .format(gversion, MIN_GO_VERSION))
    return


def parse_cli():
    curdir = os.path.abspath(os.environ.get('PWD', '.'))
    script_dir = os.path.dirname(sys.argv[0])

    parser = argparse.ArgumentParser()
    parser.set_defaults(
        curdir=curdir,
        script_dir=script_dir,
        base_dir=os.path.abspath(os.path.join(curdir, script_dir)),
        )
    parser.add_argument('--dry-run', '-n', action='store_true')
    parser.add_argument('--disable-env', '-E', action='append')
    parser.add_argument('--base-dir')
    parser.add_argument("--test-suite", "-s", default=Filter())
    parser.add_argument("--test-group", "-g", default=Filter())

    return parser.parse_args()


class ManageEnv(object):
    def __init__(self, cli, envs):
        self.envs = []
        for env in envs:
            log.info('Supported Environment: %s', env)
            if cli.dry_run:
                continue
            if cli.disable_env and env.name in cli.disable_env:
                log.info('Disabled Environment: %s', env)
                continue
            log.info('Enabling Environment: %s', env)
            self.envs.append(env)

    def __enter__(self):
        for env in self.envs:
            env.setup()
        return self

    def __exit__(self, ecls, err, tb=None):
        for env in reversed(self.envs):
            env.teardown()


def exec_suite(cli, test_group, test_suite):
    if cli.dry_run:
        return
    try:
        test_suite.run()
        error = None
    except Exception as err:
        error = str(err)
    return {
        'group': str(test_group),
        'suite': str(test_suite),
        'error': error,
    }


def process_results(results, start_time):
    failed = False
    for result in results:
        if result.get('error'):
            print ('FAILED {group} - {suite}'.format(**result))
            failed = True
        else:
            print ('PASSED {group} - {suite}'.format(**result))
    return not failed


def run(cli):
    check_prereqs()
    # See https://bugzilla.redhat.com/show_bug.cgi?id=1327740
    # _sudo setenforce 0

    results = []
    tc = TestCollection(cli.base_dir)
    # TODO: pre-clean environments
    start_time = time.time()
    log.info("Found functional test: %s", tc)
    for tg in tc.groups(filter=cli.test_group):
        log.info("Found test group: %s", tg)
        with ManageEnv(cli, tg.environments()):
            for ts in tg.suites(filter=cli.test_suite):
                log.info("Found test suite: %s", ts)
                results.append(exec_suite(cli, tg, ts))
    return process_results(results, start_time)


def main():
    cli = parse_cli()
    logging.basicConfig(
        level=logging.DEBUG)
    print (cli)
    try:
        results = run(cli)
        if results:
            sys.exit(0)
        else:
            sys.exit(1)
    except Fail as e:
        sys.stderr.write("ERROR: {}\n".format(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
