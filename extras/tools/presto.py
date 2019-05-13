
import copy
import json
import pprint
import logging
import sys

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


def load():
    hdata = parse_heketi(sys.argv[1])

    volumes = [
        Volume(
            vol_id='979c4584d2f7d263f3d5cf520eccd1c7',
            name='vol_979c4584d2f7d263f3d5cf520eccd1c7',
            cluster='44f689a535da8673f1361633f430ee10',
            size=20,
            gid=2009,
            bricks=[
                brick_from_path(hdata, '192.168.52.195:/var/lib/heketi/mounts/vg_e5f063fc30c3320d6598a10f824282b1/brick_8835c5971575db25a4d54a6ff05178a4/brick'),
                brick_from_path(hdata, '192.168.52.196:/var/lib/heketi/mounts/vg_542ebeec2fc1d191924d79879d638d90/brick_f6095807ed53a04cdd479a6bf2ae9571/brick'),
                brick_from_path(hdata, '192.168.52.197:/var/lib/heketi/mounts/vg_0c42561fb3c34471f3ea0d637a7a9f6b/brick_45b13d48ca584d8b860af3481131252a/brick'),
            ]),
        Volume(
            vol_id='d384b555d3854d2a97b02bd2e620dfdc',
            name='vol_d384b555d3854d2a97b02bd2e620dfdc',
            cluster='44f689a535da8673f1361633f430ee10',
            size=10,
            gid=2009,
            bricks=[
                brick_from_path(hdata, "192.168.52.196:/var/lib/heketi/mounts/vg_7acfa408ae9ecf7b6ca3cbfe46be64f8/brick_d2e7dc203a4d354806a9d3cb5b3e15f3/brick"),
                brick_from_path(hdata, "192.168.52.197:/var/lib/heketi/mounts/vg_4dc082d54055dcb4f31646a9e154a988/brick_b056476bfa9c736418b607a3af7fd686/brick"),
                brick_from_path(hdata, "192.168.52.195:/var/lib/heketi/mounts/vg_e5f063fc30c3320d6598a10f824282b1/brick_f3db89a97305c94177321ffdeda89b95/brick"),
            ]),
    ]
    for v in volumes:
        v.update()
    return hdata, volumes


def fixup(hdata, new_vols):
    for v in new_vols:
        hdata['volumeentries'][v.vol_id] = v.expand()
        hdata['clusterentries'][v.cluster]['Info']['volumes'].append(v.vol_id)
        for b in v.bricks:
            hdata['brickentries'][b.brick_id] = b.expand()
            hdata['deviceentries'][b.device_id]['Bricks'].append(b.brick_id)
    return hdata


def fixup2():
    log.info("Reading heketi data ...")
    hdata = parse_heketi(sys.argv[1])
    log.info("Reading gluster data ...")
    gvinfo = parse_gvinfo(sys.argv[2])
    lvinfo = parse_lv_json(*sys.argv[3:])

    vlist = hdata['volumeentries'].keys()

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


    for vid in vlist:
        name = hdata['volumeentries'][vid]['Info']['name']
        # TODO: check num of bricks in db vs gluster
        snap_factor = hdata['volumeentries'][vid]['Info']['snapshot']['factor']
        if snap_factor != 1:
            raise ValueError('can only handle snap_factor=1')
        missing_bricks = []
        for bid in hdata['volumeentries'][vid]['Bricks']:
            if bid not in hdata['brickentries']:
                missing_bricks.append(bid)
        if not missing_bricks:
            continue
        if hdata['volumeentries'][vid]['Pending'].get('Id'):
            raise ValueError('volume %r is pending' % vid)
        log.info("Missing bricks: %r", missing_bricks)
        gvol = gvinfo[name]
        vsize = hdata['volumeentries'][vid]['Info']['size']
        if vid == '2b3de9aad17fffef443c4d1215d30315':
            vsize = int(vsize / 4)
        log.info('Using gluster info: %r', gvol)
        for bpath in gvol:
            if not any(bid in bpath for bid in missing_bricks):
                continue
            log.info('Attempting to restore brick %s', bpath)
            b = brick_from_path(hdata, bpath)
            b.update(
                vol_id=vid,
                vol_size=vsize,
                tp_name=lvmap.get('brick_{}'.format(b.brick_id), ''))
            hdata['brickentries'][b.brick_id] = b.expand()
            if b.brick_id not in hdata['deviceentries'][b.device_id]['Bricks']:
                log.info('Adding brick id %r to device %r',
                         b.brick_id, b.device_id)
                hdata['deviceentries'][b.device_id]['Bricks'].append(b.brick_id)


    return hdata

if __name__ == '__main__':
    logging.basicConfig(
        stream=sys.stderr,
        format=LF,
        level=logging.DEBUG)

    j = fixup2()
    json.dump(j, sys.stdout, indent=4)
