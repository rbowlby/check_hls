#!/usr/bin/python3.2
'''
Ryan Bowlby: rbowlby83 yahoo

To Do:
    arg sanitation
    code review
'''

import os
import shutil
import tempfile
import urllib.request

class StreamError(Exception):
    ''' Custom exception with url and bandwidth attributes. '''
    def __init__(self, error_str, url, bandwidth=None):
        super().__init__(error_str)
        self.error_str = error_str
        self.url = url
        self.bandwidth = bandwidth

class Stream():
    ''' A simple HTTP Live streaming interface that allows retrieval of
        playlists (both main and variants) and transport segments. '''

    def __init__(self, host, path, port=None, ssl=False, timeout=10, token=None):
        ''' Sets initial connection attributes and retrieves main playlist
        file object. Exception raised for requests beyond timeout in seconds.'''

        self.host = '{}:{}'.format(host, port) if port else host
        self.path = '/{}{}'.format(token, path) if token else self.path
        proto = 'https' if ssl else 'http'
        self.addr = '{}://{}{}'.format(proto, self.host, self.path)
        self.timeout = timeout
        try:
            self.playlist = urllib.request.urlopen(self.addr, timeout=self.timeout)
        except urllib.error.HTTPError:
            raise StreamError('Error retrieving playlist. ', self.addr)
        except urllib.error.URLError:
            raise StreamError('Error resolving host.', self.addr)

    def get_variants(self, playlist, bandwidths='all'):
        ''' Parses playlist for variant playlists and returns a dictionary with
        the variant bandwidth as key and a tuple containing URL retrieved and
        playlist file. 

        { '200000': (http://example.com/200000.m3u8, playlist), .... }

        Lack of variants raises an exception, so check before hand.
        '''
        variants = {}
        url_next = False
        for line in playlist:
            try:
                line = line.decode('utf-8')
            except AttributeError:
                pass
            line = line.strip()
            if line.startswith('#EXT-X-STREAM-INF'):
                bandwidth = Stream._get_bandwidth(line)
                url_next = True; continue
            elif url_next:
                url_next = False
                if bandwidth in bandwidths or 'all' in bandwidths:
                    addr = Stream._build_addr(self.addr, line)
                    try:
                        variant = urllib.request.urlopen(addr, timeout=self.timeout)
                    except urllib.error.HTTPError:
                        raise StreamError(
                            'Error retrieving variant playlist ',
                            addr, bandwidth=bandwidth)
                    except urllib.error.URLError:
                        raise StreamError(
                            'Error retrieving variant playlist, host resolution issue. ',
                            addr, bandwidth=bandwidth)
                    variants[bandwidth] = (addr, variant)
        if not variants:
            raise StreamError('Playlist must contain variants, check bandwidths exist.', self.addr)
        return variants

    def retrieve_segments(self, parent_addr, playlist, duration=30, download_dir='/var/tmp'):
        ''' Retrieves N number of ts files for given playlist. Where N is
        derived by ( duration / segment content duration ). Returns a dictionary
        with url as key and a corresponding absolute file path.

            { 'http://www.example.com/01.ts': '/var/tmp/234slj98/01.ts', ... }

        Exception returned for requests beyond timeout in seconds.
        '''
        files = {}
        temp_dir = tempfile.mkdtemp(dir=download_dir)
        seg_next = False
        for line in playlist:
            try: line = line.decode('utf-8')
            except: pass
            line = line.strip()
            # determine num segments to download
            if '#EXT-X-TARGETDURATION' in line:
                self.seg_duration = int(line.split(':')[1])
                num_segments = duration // self.seg_duration
                # download at least one
                num_segments = num_segments if num_segments else 1
            elif line.startswith('#EXTINF'):
                # next iter will be a segment to download
                seg_next = True; continue
            elif seg_next:
                seg_next = False
                if num_segments == 0: break
                addr = Stream._build_addr(parent_addr, line)
                file = '{}/{}'.format(temp_dir, line.split('/')[-1])
                try:
                    urllib.request.urlretrieve(addr, file)
                except (urllib.error.URLError, ValueError) :
                    shutil.rmtree(temp_dir)
                    raise StreamError('Error retrieving segment ', addr)
                except:
                    shutil.rmtree(temp_dir)
                    raise
                files[addr] = file
                #print(file)
                num_segments -= 1
        return files

    def check_size(self, file):
        ''' Checks that segment (file) is of a certain size, returns True or
        raises ValueError. '''
        # assume a valid ts file is at least 1000B per sec of content
        min_size = 1000 * self.seg_duration
        size = os.path.getsize(file)
        if size < min_size:
            raise ValueError('Critical: {}sec segment under {}bytes: {}'.format(
                self.seg_duration, min_size, file))
        else:
            return True

    def _get_bandwidth(line):
        ''' Retrieve bandwidth num from #EXT-X-STREAM-INF tag. '''
        for part in line.split(','):
            if 'BANDWIDTH' in part:
                return(part.split('=')[1])

    def _build_addr(parent, child):
        ''' Takes an absolute url as parent and a child url that may be
        relative. Returns an absolute url to path specified in child using
        parent domain if necessary. '''
        if not child.startswith('http'):
            if child.startswith('/'):
                base_name = 'http://{}'.format(parent.split('/')[2])
            else:
                base_name = '/'.join(parent.split('/')[:-1])
            child = '{}/{}'.format(base_name, child)
        return child
