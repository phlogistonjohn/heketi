
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
        if 'lv_size' in kwargs:
            lv_size = clean_lv_size(kwargs['lv_size'])
            self.size = lv_size
            self.tp_size = lv_size
            self.pmd_size = calc_pmd_size(lv_size, check=kwargs['vol_size'])
        if 'tp_name' in kwargs:
            self.tp_name = kwargs['tp_name']
            if self.brick_id not in self.tp_name:
                log.warning('saw differing tp_name=%r', self.tp_name)

    def __repr__(self):
        return 'Brick'+repr(vars(self))


def clean_lv_size(txt):
    if not txt.endswith('.00k'):
        raise ValueError('unexpected size: {}'.format(txt))
    s = txt.split('.')[0]
    return int(s)


def brick_path_id(bp):
    segments = bp.split('/')
    for s in segments:
        if s.startswith('brick_'):
            return s[6:]
    raise ValueError(bp)


def brick_path_info(bp):
    ip = device_id = brick_id = None
    ip, bp = bp.split(':')
    parts = bp.split('/')
    for part in parts:
        if part.startswith('vg_'):
            device_id = part[3:]
        if part.startswith('brick_'):
            brick_id = part[6:]
    return ip, device_id, brick_id


def node_id_from_ip(hdata, ip):
    node_id = None
    for n in hdata['nodeentries'].values():
        hn = n['Info']['hostnames']
        if ip in hn['storage'] or ip in hn['manage']:
            node_id = n["Info"]["id"]
    return node_id


def brick_from_path(hdata, bp):
    b = Brick()
    ip, device_id, brick_id = brick_path_info(bp)
    b.device_id = device_id
    b.brick_id = brick_id
    b.node_id = node_id_from_ip(hdata, ip)
    return b


def brick_from_brick(hdata, old_id, bp):
    b = Brick()
    orig = hdata['brickentries'][old_id]
    new_ip, new_device_id, new_id = brick_path_info(bp)
    b.brick_id = new_id
    b.device_id = new_device_id
    b.node_id = node_id_from_ip(hdata, new_ip)
    b.size = orig['Info']['size']
    b.tp_size = orig['TpSize']
    b.pmd_size = orig['PoolMetadataSize']
    b.tp_name = ''
    b.vol_id = orig['Info']['volume']
    return b


PMD_TABLE = {
    1: 8192,
    2: 12288,
    3: 16384,
    5: 28672,
    8: 45056,
    10: 53248,
    15: 81920,
    20: 106496,
    25: 131072,
    45: 237568,
    50: 262144,
    80: 421888,
    100: 524288,
}

EXTENT_SIZE = 4096
MAX_PMD_SIZE = 16 * (1024 * 1024)


def calc_pmd_size(tpsize, check=None):
    alignment = tpsize % EXTENT_SIZE
    if alignment != 0:
        raise ValueError("alignment!")

    metadataSize = int(float(tpsize) * 0.005)
    if metadataSize > MAX_PMD_SIZE:
        metadataSize = MAX_PMD_SIZE

    alignment = metadataSize % EXTENT_SIZE
    if alignment != 0:
        metadataSize += EXTENT_SIZE - alignment

    if check and PMD_TABLE[check]:
        log.info("checking PoolMetadataSize against table")
        if PMD_TABLE[check] != metadataSize:
            log.warning('PoolMetadataSize not as exptected, got %s, expected %s',
                metadataSize, PMD_TABLE[check])
    return metadataSize


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
                    "10.47.60.18",
                    "10.47.60.20",
                    "10.47.60.19"
                ],
                "device": "10.47.60.18:{name}",
                "options": {
                    "backup-volfile-servers": "10.47.60.20,10.47.60.19"
                }
            }
        },
        "blockinfo": {}
    },
    "Bricks": [
    ],
    "GlusterVolumeOptions": [
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
    for jfile in jfiles:
        log.info("Reading lv json: %r", jfile)
        with open(jfile) as fh:
            j = json.load(fh)
        lv_inner = j["report"][0]["lv"]
        lv.extend(lv_inner)
    return lv


def parse_oshift(yf):
    with open(yf) as fh:
        return yaml.safe_load(fh)


def parse_brick_swap(bsfile):
    bswap = {}
    with open(bsfile) as fh:
        for line in fh:
            l = line.strip()
            if not l or l.startswith('#'):
                continue
            nbrick, obrick = l.split()
            bswap[nbrick] = obrick
    return bswap


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


def restore_bricks(hdata, gvinfo, pvdata, lvinfo, brick_swap, increase_usage=True):

    lvmap = {}
    lvs = {}
    diffcount = 0
    for lv in lvinfo:
        lv_name = lv['lv_name']
        if lv_name.startswith('brick_'):
            pool_lv = lv['pool_lv']
            lvmap[lv_name] = pool_lv
            if lv_name.split('_')[1] != pool_lv.split('_')[1]:
                diffcount += 1
            lvs[lv_name] = lv
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
            _, _, brick_id = brick_path_info(bpath)
            if brick_id in brick_swap:
                log.info("Sourcing brick settings from %s", brick_swap[brick_id])
                b = brick_from_brick(hdata, brick_swap[brick_id], bpath)
            else:
                b = brick_from_path(hdata, bpath)
            brick_key = 'brick_{}'.format(b.brick_id)
            b.update(
                vol_id=vid,
                vol_size=vsize,
                lv_size=lvs.get(brick_key, {}).get('lv_size', ''),
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
                if increase_usage:
                    d = hdata['deviceentries'][b.device_id]
                    bsize = b.tp_size + b.pmd_size
                    d["Info"]["storage"]["free"] -= bsize
                    d["Info"]["storage"]["used"] += bsize
    return hdata


def trim_bricks(hdata, brick_ids, reduce_usage=True):
    for brick_id in brick_ids:
        b = hdata['brickentries'].pop(brick_id)
        for v in hdata['volumeentries'].values():
            if brick_id in v['Bricks']:
                v['Bricks'].remove(brick_id)
        for d in hdata['deviceentries'].values():
            if brick_id in d['Bricks']:
                d['Bricks'].remove(brick_id)
                if reduce_usage:
                    bsize = b["TpSize"] + b["PoolMetadataSize"]
                    d["Info"]["storage"]["free"] += bsize
                    d["Info"]["storage"]["used"] -= bsize
    return hdata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--gluster-info', '-g',
        required=True,
        help='Path to a file containing gluster volume info')
    parser.add_argument(
        '--heketi-json', '-j',
        required=True,
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
    parser.add_argument(
        '--brick-swap', '-b',
        help='Brick id to brick id mapping for brick replacement')
    parser.add_argument(
        '--trim-swapped-bricks', '-T', action='store_true',
        help='Trim swapped source bricks from db')
    args = parser.parse_args()

    log.info("Reading heketi data ...")
    hdata = parse_heketi(args.heketi_json)
    log.info("Reading gluster data ...")
    gvinfo = parse_gvinfo(args.gluster_info)
    if args.pv_yaml:
        log.info("Reading PV yaml ...")
        pvdata = parse_oshift(args.pv_yaml)
    else:
        pvdata = {"items": []}
    log.info("Reading lvs json ...")
    lvdata = parse_lv_json(*(args.lvs_json or []))
    if args.brick_swap:
        log.info("Reading brick swap...")
        brick_swap = parse_brick_swap(args.brick_swap)
    else:
        brick_swap = {}
    if args.volume:
        restore_volumes(hdata, gvinfo, pvdata, lvdata, args.volume)
    restore_bricks(hdata, gvinfo, pvdata, lvdata, brick_swap)
    if args.trim_swapped_bricks:
        trim_bricks(hdata, brick_swap.values())
    json.dump(hdata, sys.stdout, indent=4)


if __name__ == '__main__':
    logging.basicConfig(
        stream=sys.stderr,
        format=LF,
        level=logging.DEBUG)
    main()
