#!/usr/bin/python3

import argparse
import copy
import json
import logging
import re
import sys


DESC = """
Create a new updated heketi json where bricks have been swapped.
"""

EXAMPLE = """
"""


LF = '%(asctime)s: %(levelname)s: %(message)s'
log = logging.getLogger('scrub')


class CliError(ValueError):
    pass


def swap_spec(txt):
    parts = re.split('[ ,/:]', txt)
    if len(parts) != 4:
        raise ValueError('Swap spec requires 4 ids')
    return parts


def parse_heketi(h_json):
    with open(h_json) as fh:
        return json.load(fh)


def parse_swap_file(fname):
    swaps = []
    with open(fname) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith('#'):
                swaps.append(swap_spec(line))
    return swaps


def parse_tpmap(*lv_json):
    tpmap = {}
    for j in lv_json:
        with open(j) as fh:
            lvj = json.load(fh)
            for r in lvj['report']:
                for lv in r['lv']:
                    l = lv['lv_name']
                    p = lv['pool_lv']
                    if l.startswith('brick_') and p:
                        b = l.split('_', 1)[1]
                        tpmap[b] = p
    return tpmap


def swap_brick(heketi, tpmap, old_device, old_brick, new_device, new_brick):
    if old_device not in heketi['deviceentries']:
        raise CliError('unknown device id: {}'.format(old_device))
    if new_device not in heketi['deviceentries']:
        raise CliError('unknown device id: {}'.format(new_device))
    if old_brick not in heketi['brickentries']:
        raise CliError('unknown brick id: {}'.format(old_brick))
    if new_brick in heketi['brickentries']:
        raise CliError('can not use existing brick id: {}'.format(new_brick))

    log.info('Will swap brick %s with %s',
             old_brick, new_brick)
    bold = heketi['brickentries'][old_brick]
    bnew = copy.deepcopy(bold)
    bnew['Info']['id'] = new_brick
    bnew['Info']['path'] = (
        '/var/lib/heketi/mounts/vg_{}/brick_{}/brick'.format(new_device, new_brick))
    bnew['Info']['device'] = new_device
    bnew['Info']['node'] = heketi['deviceentries'][new_device]["NodeId"]
    if bnew['LvmThinPool']:
        if new_brick not in tpmap:
            raise CliError("brick not in thin-pool map, can not guess at thin pool")
        bnew['LvmThinPool'] = tpmap[new_brick]

    # stitch in new brick
    heketi['brickentries'][new_brick] = bnew
    heketi['deviceentries'][new_device]['Bricks'].append(new_brick)
    heketi['volumeentries'][bnew['Info']['volume']]['Bricks'].append(new_brick)

    # remove old brick
    del heketi['brickentries'][old_brick]
    heketi['deviceentries'][old_device]['Bricks'].remove(old_brick)
    heketi['volumeentries'][bnew['Info']['volume']]['Bricks'].remove(old_brick)

    # update device sizes
    ssold = heketi['deviceentries'][old_device]['Info']['storage']
    ssnew = heketi['deviceentries'][new_device]['Info']['storage']
    bsize = bnew["TpSize"] + bnew["PoolMetadataSize"]
    ssold['used'] -= bsize
    ssold['free'] += bsize
    ssnew['used'] += bsize
    ssnew['free'] -= bsize

    return


def swap_bricks(cli):
    heketi = parse_heketi(cli.heketi_json)
    tpmap = parse_tpmap(*(cli.lv_json or []))

    swaps = list(cli.swap or [])
    if cli.swap_file:
        swaps.extend(parse_swap_file(cli.swap_file))

    for swap in swaps:
        swap_brick(heketi, tpmap, *swap)

    json.dump(heketi, sys.stdout, indent=4)
    sys.stdout.write('\n')


def main():
    parser = argparse.ArgumentParser(description=DESC, epilog=EXAMPLE)
    parser.add_argument(
        '--heketi-json', '-j',
        help='Path to a file containing Heketi db json export')
    parser.add_argument(
        '--lv-json', '-l',
        action='append',
        help='Path to a file containing lvs json')
    parser.add_argument(
        '--swap-file', '-f',
        help='File containing list of vg id, brick id, new vg id, new brick id to swap')
    parser.add_argument(
        '--swap', '-s',
        action='append', type=swap_spec,
        help='Set of vg id, brick id, new vg id, new brick id to swap')

    logging.basicConfig(
        stream=sys.stderr,
        format=LF,
        level=logging.DEBUG)
    cli = parser.parse_args()
    try:
        if not cli.heketi_json:
            raise CliError("heketi json is required")
        return swap_bricks(cli)
    except CliError as err:
        parser.error(str(err))


if __name__ == '__main__':
    main()
