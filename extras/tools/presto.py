
import argparse
import copy
import json
import logging
import pprint
import sys
import yaml

LF = '%(asctime)s: %(levelname)s: %(message)s'
log = logging.getLogger('presto')


class Volume(object):
    vol_id = ''
    name = ''
    cluster = ''
    size = 0
    gid = 0
    bricks = []

    def __init__(self, vol_id, name, cluster, size, gid, bricks):
        self.vol_id = vol_id
        self.name = name
        self.cluster = cluster
        self.size = size
        self.gid = gid
        self.bricks = bricks

    def expand(self):
        d = copy.deepcopy(VOL_STUB)
        d['Info']['size'] = self.size
        d['Info']['name'] = self.name
        d['Info']["id"] = self.vol_id
        d['Info']["cluster"] = self.cluster
        v = d['Info']['mount']['glusterfs']['device']
        d['Info']['mount']['glusterfs']['device'] = v.format(name=self.name)
        d['Info']['gid'] = self.gid
        d['Bricks'] = [b.brick_id for b in self.bricks]
        return d

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        for b in self.bricks:
            b.update(vol_id=self.vol_id, vol_size=self.size)

    def __repr__(self):
        return 'Volume({vol_id}, {name}, {cluster}, {size}, {bricks})'.format(**vars(self))


class Brick(object):
    brick_id = ''
    device_id = ''
    node_id = ''
    size = 0
    tp_size = 0
    pmd_size = 0
    tp_name = ''
    vol_id = ''

    def expand(self):
        d = copy.deepcopy(BRICK_STUB)
        d['Info']['id'] = self.brick_id
        v = d['Info']['path']
        d['Info']['path'] = v.format(**vars(self))
        d['Info']["device"] = self.device_id
        d['Info']["node"] = self.node_id
        d['Info']["volume"] = self.vol_id
        d['Info']["size"] = self.size
        d["TpSize"] = self.tp_size
        d["PoolMetadataSize"] = self.pmd_size
        d["LvmThinPool"] = self.tp_name
        return d

    def update(self, **kwargs):
        if 'vol_id' in kwargs:
            self.vol_id = kwargs['vol_id']
        if 'vol_size' in kwargs and self.size == 0:
            vs = kwargs['vol_size']
            self.size = vs * (1024 * 1024)
            self.tp_size = self.size
            self.pmd_size = PMD_TABLE[vs]
        if 'tp_name' in kwargs:
            self.tp_name = kwargs['tp_name']
            if self.brick_id not in self.tp_name:
                log.warning('saw differing tp_name=%r', self.tp_name)

    def __repr__(self):
        return 'Brick'+repr(vars(self))


def brick_path_id(bp):
    segments = bp.split('/')
    for s in segments:
        if s.startswith('brick_'):
            return s[6:]
    raise ValueError(bp)


def brick_from_path(hdata, bp):
    b = Brick()
    ip, bp = bp.split(':')
    parts = bp.split('/')
    for part in parts:
        if part.startswith('vg_'):
            b.device_id = part[3:]
        if part.startswith('brick_'):
            b.brick_id = part[6:]

    for n in hdata['nodeentries'].values():
        hn = n['Info']['hostnames']
        if ip in hn['storage'] or ip in hn['manage']:
            b.node_id = n["Info"]["id"]
    return b


PMD_TABLE = {
    1: 8192,
    2: 12288,
    5: 28672,
    10: 53248,
    15: 81920,
    20: 106496,
    25: 131072,
    50: 262144,
    80: 421888,
    100: 524288,
}


VOL_STUB = {
    "Info": {
        "size": 0,
        "name": "{name}",
        "durability": {
            "type": "replicate",
            "replicate": {
                "replica": 3
            },
            "disperse": {}
        },
        "gid": 0,
        "snapshot": {
            "enable": True,
            "factor": 1
        },
        "id": "{vol_id}",
        "cluster": "{cluster}",
        "mount": {
            "glusterfs": {
                "hosts": [
                    "192.168.52.196",
                    "192.168.52.197",
                    "192.168.52.195"
                ],
                "device": "192.168.52.196:{name}",
                "options": {
                    "backup-volfile-servers": "192.168.52.197,192.168.52.195"
                }
            }
        },
        "blockinfo": {}
    },
    "Bricks": [
    ],
    "GlusterVolumeOptions": [
        "server.tcp-user-timeout 42",
        ""
    ],
    "Pending": {
        "Id": ""
    }
}

BRICK_STUB = {
    "Info": {
        "id": "{brick_id}",
        "path": "/var/lib/heketi/mounts/vg_{device_id}/brick_{brick_id}/brick",
        "device": "{device_id}",
        "node": "{node_id}",
        "volume": "{vol_id}",
        "size": 0
    },
    "TpSize": 0,
    "PoolMetadataSize": 0,
    "Pending": {
        "Id": ""
    },
    "LvmThinPool": "{tp_name}",
    "LvmLv": "",
    "SubType": 1
}


def parse_heketi(h_json):
    with open(h_json) as fh:
        return json.load(fh)


def parse_gvinfo(gvi):
    vols = {}
    volume = None
    with open(gvi) as fh:
        for line in fh:
            l = line.strip()
            if l.startswith("Volume Name:"):
                volume = l.split(":", 1)[-1].strip()
                vols[volume] = []
            if l.startswith('Brick') and l != "Bricks:":
                if volume is None:
                    raise ValueError("Got Brick before volume: %s" % l)
                vols[volume].append(l.split(":", 1)[-1].strip())
    return vols


def parse_lv_json(*jfiles):
    lv = []
    log.info("Reading lv json: %r", jfiles)
    for jfile in jfiles:
        with open(jfile) as fh:
            j = json.load(fh)
        lv_inner = j["report"][0]["lv"]
        lv.extend(lv_inner)
    return lv


def parse_oshift(yf):
    with open(yf) as fh:
        return yaml.safe_load(fh)


def pv_vol_map(pvdata):
    m = {}
    for entry in pvdata['items']:
        try:
            key = entry['spec']['glusterfs']['path']
        except KeyError:
            continue
        log.debug("Found gluster volume in pv: %s", key)
        m[key] = entry
    return m


def restore_volumes(hdata, gvinfo, pvdata, lvdata, vols):
    pvmap = pv_vol_map(pvdata)

    if len(hdata['clusterentries']) > 1:
        raise ValueError("currently requires one cluster in heketi")
    cluster_id = list(hdata['clusterentries'].keys())[0]

    for volname in vols:
        pv = pvmap[volname]
        try:
            vol_id = pv['metadata']['annotations']['gluster.kubernetes.io/heketi-volume-id']
        except KeyError:
            log.error("did not find volume id for volume: %s", volname)
            raise
        v = Volume(
            vol_id=vol_id,
            name=volname,
            cluster=cluster_id,
            size=int(pv['spec']['capacity']['storage'].replace("Gi", "")),
            gid=int(pv['metadata']['annotations'].get("pv.beta.kubernetes.io/gid", 0)),
            bricks=[])

        hdata['volumeentries'][v.vol_id] = v.expand()
        hdata['clusterentries'][v.cluster]['Info']['volumes'].append(v.vol_id)
        for b in v.bricks:
            hdata['brickentries'][b.brick_id] = b.expand()
            hdata['deviceentries'][b.device_id]['Bricks'].append(b.brick_id)


def restore_bricks(hdata, gvinfo, pvdata, lvinfo):

    lvmap = {}
    diffcount = 0
    for lv in lvinfo:
        lv_name = lv['lv_name']
        if lv_name.startswith('brick_'):
            pool_lv = lv['pool_lv']
            lvmap[lv_name] = pool_lv
            if lv_name.split('_')[1] != pool_lv.split('_')[1]:
                diffcount += 1
    log.info('differing lv brick id / pool id count = %s', diffcount)

    for vid in hdata['volumeentries'].keys():
        log.debug("Checking %s for missing bricks", vid)
        name = hdata['volumeentries'][vid]['Info']['name']
        snap_factor = hdata['volumeentries'][vid]['Info']['snapshot']['factor']
        if snap_factor != 1:
            raise ValueError('can only handle snap_factor=1')
        missing_bricks = set()
        try:
            gvol = gvinfo[name]
        except KeyError:
            log.error(
                "No entry for volume %s in gluster volume info, skipping",
                name)
            continue
        for bpath in gvol:
            bid = brick_path_id(bpath)
            if bid not in hdata['volumeentries'][vid]['Bricks']:
                missing_bricks.add(bid)
        for bid in hdata['volumeentries'][vid]['Bricks']:
            if bid not in hdata['brickentries']:
                missing_bricks.add(bid)
        if not missing_bricks:
            log.info("No missing bricks for %s", vid)
            continue
        log.info("Missing bricks for volume %s", vid)
        if hdata['volumeentries'][vid]['Pending'].get('Id'):
            raise ValueError('volume %r is pending' % vid)
        log.info("Missing bricks: %r", sorted(missing_bricks))
        vsize = hdata['volumeentries'][vid]['Info']['size']
        if vid == '2b3de9aad17fffef443c4d1215d30315':
            vsize = int(vsize / 4)
        log.info('Using gluster info: %r', gvol)
        for bpath in gvol:
            if not any(bid in bpath for bid in sorted(missing_bricks)):
                continue
            log.info('Attempting to restore brick %s', bpath)
            b = brick_from_path(hdata, bpath)
            b.update(
                vol_id=vid,
                vol_size=vsize,
                tp_name=lvmap.get('brick_{}'.format(b.brick_id), ''))
            hdata['brickentries'][b.brick_id] = b.expand()
            if b.brick_id not in hdata['volumeentries'][vid]['Bricks']:
                log.info('Adding brick id %r to volume %r',
                         b.brick_id, vid)
                hdata['volumeentries'][vid]['Bricks'].append(b.brick_id)
            if b.brick_id not in hdata['deviceentries'][b.device_id]['Bricks']:
                log.info('Adding brick id %r to device %r',
                         b.brick_id, b.device_id)
                hdata['deviceentries'][b.device_id]['Bricks'].append(b.brick_id)
    return hdata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--gluster-info', '-g',
        help='Path to a file containing gluster volume info')
    parser.add_argument(
        '--heketi-json', '-j',
        help='Path to a file containing Heketi db json export')
    parser.add_argument(
        '--pv-yaml', '-y',
        help='Path to a file containing PV yaml data')
    parser.add_argument(
        '--lvs-json', '-l', action='append',
        help='LVM LVs json')
    parser.add_argument(
        '--volume', '-V', action='append',
        help='Restore volume with given id & name')
    args = parser.parse_args()

    log.info("Reading heketi data ...")
    hdata = parse_heketi(args.heketi_json)
    log.info("Reading gluster data ...")
    gvinfo = parse_gvinfo(args.gluster_info)
    log.info("Reading PV yaml ...")
    pvdata = parse_oshift(args.pv_yaml)
    log.info("Reading lvs json ...")
    lvdata = parse_lv_json(*args.lvs_json)
    if args.volume:
        restore_volumes(hdata, gvinfo, pvdata, lvdata, args.volume)
    restore_bricks(hdata, gvinfo, pvdata, lvdata)
    json.dump(hdata, sys.stdout, indent=4)


if __name__ == '__main__':
    logging.basicConfig(
        stream=sys.stderr,
        format=LF,
        level=logging.DEBUG)
    main()
