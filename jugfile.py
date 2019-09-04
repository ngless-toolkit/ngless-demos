from jug import TaskGenerator
from os import makedirs, path
from collections import namedtuple

Demo = namedtuple('Demo', ['demo_name', 'demo_script', 'sample_file', 'samples'])

ENA_BASE_URL = 'http://ftp.sra.ebi.ac.uk/vol1/fastq/'
NR_READS = 250_000

@TaskGenerator
def download_file(url, ofname):
    from os import makedirs, path
    if '/' in ofname:
        makedirs(path.dirname(ofname), exist_ok=True)

    import requests
    with open(ofname, 'wb') as output:
        r = requests.get(url, stream=True)
        for c in r.iter_content(chunk_size=8192 * 1024):
            output.write(c)
    return ofname

@TaskGenerator
def sample_fastq(ifname, ofname, nr_reads):
    from os import makedirs, path
    if '/' in ofname:
        makedirs(path.dirname(ofname), exist_ok=True)
    import gzip

    with gzip.open(ofname, 'wt') as ofile:
        with gzip.open(ifname, 'rt') as ifile:
            for i, line in enumerate(ifile):
                if i >= nr_reads*4:
                    return ofname
                ofile.write(line)
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
             sample_file='tara.demo.short',
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
        })]


for demo in DATA:
    makedirs(demo.demo_name, exist_ok=True)
    copy_file(demo.demo_script, demo.demo_name)
    for s,fs in demo.samples.items():
        for f in fs:
            fname = path.basename(f)
            ofname = f'{demo.demo_name}/{s}.sampled/{fname}'.replace('.fastq.gz', '.short.fq.gz')
            tofname = f'{demo.demo_name}-raw-data/{s}/{fname}'
            # Downloading and sampling should be done as a single step, avoiding  the big intermediate files
            tofname = download_file(ENA_BASE_URL + f, tofname)
            sample_fastq(tofname, ofname, NR_READS)
    generate_sample_file(demo.demo_name, demo.sample_file, demo.samples)



