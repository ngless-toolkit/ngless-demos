from jug import TaskGenerator
from os import makedirs, path
from collections import namedtuple

Demo = namedtuple('Demo', ['demo_name', 'demo_script', 'sample_file', 'samples'])

ENA_BASE_URL = 'http://ftp.sra.ebi.ac.uk/vol1/fastq/'
NR_READS = 250_000

def gunzip_request(url):
    import requests
    import zlib
    # From the zlib documentation for the decompressobj
    # +40 to +47 = 32 + (8 to 15):
    #      Uses the low 4 bits of the value as the window size logarithm, and
    #      automatically accepts either the zlib or gzip format.
    dec = zlib.decompressobj(47)
    r = requests.get(url, stream=True)
    for c in r.iter_content(chunk_size=8192 * 1024):
        yield dec.decompress(c)
    yield dec.flush()

def breakup_lines(istream):
    prev = b''
    for ch in istream:
        tokens = ch.split(b'\n')
        if not tokens:
            continue
        tokens[0] = prev + tokens[0]
        prev = tokens[-1]
        tokens = tokens[:-1]
        for tk in tokens:
            yield tk.decode('ascii') + '\n'
    if prev:
        yield prev.decode('ascii') + '\n'

@TaskGenerator
def download_sample_fastq(url, ofname, nr_reads):
    from os import makedirs, path
    import gzip
    if '/' in ofname:
        makedirs(path.dirname(ofname), exist_ok=True)

    with gzip.open(ofname, 'wt') as ofile:
        for i,line in enumerate(breakup_lines(gunzip_request(url))):
            if i >= nr_reads*4:
                print("DONE ENOUGH")
                return ofname
            ofile.write(line)
    print("EOF")
    return ofname

@TaskGenerator
def copy_file(fname, tdir):
    with open(f'{tdir}/{fname}', 'wb') as ofile:
        with open(fname, 'rb') as ifile:
            while True:
                chunk = ifile.read(8192 * 1024)
                if not chunk:
                    break
                ofile.write(chunk)

@TaskGenerator
def generate_sample_file(demo_name, sample_file, samples):
    with open(f'{demo_name}/{sample_file}', 'wt') as ofile:
        for s in samples:
            ofile.write(f'{s}.sampled\n')

DATA = [Demo(demo_name='ocean-short',
             demo_script='ocean-demo.ngl',
             sample_file='tara.demo.sampled',
             samples={
                'SAMEA2621229': [
                    'ERR594/ERR594391/ERR594391_1.fastq.gz',
                    'ERR594/ERR594391/ERR594391_2.fastq.gz',
                    ],
                'SAMEA2621155': [
                    'ERR599/ERR599133/ERR599133_1.fastq.gz',
                    'ERR599/ERR599133/ERR599133_2.fastq.gz',
                    ],
                'SAMEA2621033': [
                    'ERR594/ERR594391/ERR594391_1.fastq.gz',
                    'ERR594/ERR594391/ERR594391_2.fastq.gz',
                    ],
        }),
        Demo(demo_name='gut-short',
             demo_script='gut-demo.ngl',
             sample_file='igc.demo.short',
             samples={
                 'SAMEA104445453': [
                    'ERR222/008/ERR2227338/ERR2227338_1.fastq.gz',
                    'ERR222/008/ERR2227338/ERR2227338_2.fastq.gz',
                    'ERR222/009/ERR2227339/ERR2227339_1.fastq.gz',
                    'ERR222/009/ERR2227339/ERR2227339_2.fastq.gz',
                    ],
                'SAMEA104445455': [
                    'ERR222/002/ERR2227342/ERR2227342_1.fastq.gz',
                    'ERR222/002/ERR2227342/ERR2227342_2.fastq.gz',
                    ],
                'SAMEA104445454': [
                    'ERR222/006/ERR2227346/ERR2227346_1.fastq.gz',
                    'ERR222/006/ERR2227346/ERR2227346_2.fastq.gz',
                    ]
                 })
     ]


for demo in DATA:
    makedirs(demo.demo_name, exist_ok=True)
    copy_file(demo.demo_script, demo.demo_name)
    for s,fs in demo.samples.items():
        for f in fs:
            fname = path.basename(f)
            ofname = f'{demo.demo_name}/{s}.sampled/{fname}'.replace('.fastq.gz', '.short.fq.gz')
            download_sample_fastq(ENA_BASE_URL + f, ofname, NR_READS)
    generate_sample_file(demo.demo_name, demo.sample_file, demo.samples)



