#!/usr/bin/python3.2
'''
To Do:

look into simplifying exception handling with a function or decorator
verify timeout exceptions work
code review
'''

from hls import Stream, StreamError
import sys
import urllib.request
import shutil
import argparse

def get_args():
    ''' parse and return command line args '''

    desc = '''
    Check an HTTP Live stream for rfc compliance and segment availability
    within optional response time thresholds.'''

    example = '''
    Examples:

        check_hls -H example.com -u /path/to/stream
        check_hls --host example.com -u /path -p 8080 --ssl
        check_hls -H example.com -u /path -v H3 H5 H8 --duration=60
        '''

    parser = argparse.ArgumentParser( description = desc, epilog = example,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-H', '--host',
                        dest = 'host',
                        required = True,
                        metavar = '',
                        help = 'Address of streaming server')
    parser.add_argument('-u', '--url',
                        dest = 'url',
                        required = True,
                        metavar = '',
                        help = 'URL to retrieve stream')
    parser.add_argument('-s', '--ssl',
                        dest = 'ssl',
                        action='store_true',
                        help = 'enable https')
    parser.add_argument('-p', '--port',
                        dest = 'port',
                        default = None,
                        metavar = '',
                        help = 'Port number (default: 80)')
    parser.add_argument('-t', '--timeout',
                        dest = 'timeout',
                        default = 10,
                        type = int,
                        metavar = '',
                        help = 'Timeout in sec, for m3u8 not ts (default: 10).')
    parser.add_argument('-b', '--bandwidths',
                        dest = 'bandwidths',
                        default = 'all',
                        metavar = '',
                        nargs = '*',
                        help = 'Encoding variants by bandwidth as specified\
                        within #EXT-X-STREAM-INF tag of playlist (default: all)')
    parser.add_argument('-d', '--duration',
                        dest = 'duration',
                        default = 30,
                        type = int,
                        metavar = '',
                        help = 'Stream length in seconds (default: 30).\
                        If each segment is 10s and duration is set to 30\
                        then 3 segments are downloaded.')

    args = parser.parse_args()
    return args


def urllize(url):
    ''' returns urllized url '''
    return '<a href="{}">{}</a>'.format(url, url)


def clean(dir):
    ''' Expects list of dirs to remove. '''
    try:
        shutil.rmtree(dir)
    except OSError as e:
        return False
    return True


if __name__ == '__main__':
    args = get_args()

    try:
        stream = Stream(args.host, args.url, port=args.port,
                        timeout=args.timeout, ssl=args.ssl)
    except StreamError as e:
        print('Critical: {} {}'.format(e.error_str, urllize(e.url)))
        sys.exit(2)

    # retrieve playlist
    playlist = stream.playlist.read().decode('utf-8').splitlines()

    files = {}
    # get variant playlists if they exist in playlist
    if '#EXT-X-STREAM-INF' in ''.join(playlist):
        try:
            variants = stream.get_variants(playlist, args.bandwidths)
        except StreamError as e:
            print('Critical: {} {} bandwidth: {}.'.format(e.error_str, urllize(e.url), e.bandwidth))
            sys.exit(2)

        for bandwidth in variants: 
            variant_addr, variant_playlist = variants[bandwidth]
            try:
                files.update(stream.retrieve_segments(variant_addr, variant_playlist, duration=args.duration))
            except StreamError as e:
                print('Critical: {} {}'.format(e.error_str, urllize(e.url)))
                sys.exit(2)
    else: # no variants, so get segments for initial playlist
        try:
            files.update(stream.retrieve_segments(stream.addr, playlist, duration=args.duration))
        except StreamError as e:
            print('Critical: {} {}'.format(e.error_str, urllize(e.url)))
            sys.exit(2)

    # check file size of each segment
    tmp_dirs = []
    for addr in files:
        try:
            stream.check_size(files[addr])
            # collect tmp_dirs for removal
            dirname = '/'.join(files[addr].split('/')[:-1])
            if dirname not in tmp_dirs:
                tmp_dirs.append(dirname)
        except ValueError as e:
            print(e)
            sys.exit(2)

    # remove files
    for dir in tmp_dirs:
        if not clean(dir):
            print('Critial: unable to remove temporary directories: {}'.format(dir))
            sys.exit(2)

    print('Success: {} is up'.format(urllize(stream.addr)))
