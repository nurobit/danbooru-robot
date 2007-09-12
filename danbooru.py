#!/usr/bin/env python

'''
danbooru.py (http://untu.ms/danbooru/)
======================================
A content retrieval tool for danbooru (http://danbooru.donmai.us/). The
requirements are Python 2.5 (http://python.org/) and a little console-fu.

usage examples
==============
 * danbooru.py negima "cat ears"
   Download content tagged negima and cat_ears to the default folder
   (negima+cat_ears)
 * danbooru.py -x himm -r safe "sawatari izumi"
   Download content tagged sawatari_izumi and rated safe (-r or --rating) to
   a folder named himm (-f or --folder)
 * danbooru.py -l 50 gif
   Download content tagged as gif, limiting it to 50 posts (-l or --limit)
 * danbooru.py -i -n -s 8 flash
   Download content tagged flash, ignoring the youngest local file (or last
   id, refers to the -i or --no-last-id option), ignore whether the file
   exists in the local database (-n or --no-db), and use server 8 (-s or
   --server option, use -L or --list to see a list of available servers)
 * danbooru.py -c * -x *
   Catalogue (-c or --catalogue) and rename (-x or --fix) all files in all
   subfolders in the current path
 * danbooru.py -h
   View a list of available commands

version history
===============
 * 0.2: 09.01.2007
 * 0.1: 01.01.2007

copyright
=========
danbooru.py is made by Reinis Ivanovs (dabas@untu.ms) and is released
to the public domain.
'''

import re
import os
import urllib
import shelve
import sqlite3
import pickle

from glob import glob, iglob
from hashlib import md5
from sys import platform, stderr
from time import time
from xml.dom import minidom

# time.clock is more granual than time.time on win32
if platform == 'win32':
    from time import clock as xtime, sleep
else:
    from time import time as xtime, sleep

# A tender age
__version__ = '0.2'
__build__ = '%build%'

case = lambda count, word: word if count == 1 else word + 's'
cases = lambda count, singular, plural: singular if count == 1 else plural


# Identify as danbooru.py/0.x (change this if you want to go ninja)
class Opener(urllib.FancyURLopener):
    #~ version = 'danbooru.py/%s' % (__version__,)
    version = 'telnet 80'
urllib._urlopener = Opener()


class ServerIdError(KeyError):
    '''No such server ID'''


class Robot(dict):
    #~ api_url = 'http://danbooru.donmai.us/api/'
    api_url = 'http://miezaru.donmai.us/'
    posts_path = 'post/index.xml?tags=%(tags)s%(rating)s&limit=\
%(limit)d&offset=%(offset)d'
    last_id = '+after_id:%d'
    rating_path = '+rating:%s'
    servers_path = 'find_servers'
    md5_path = 'find_posts?md5=%s'
    settings_filename = os.path.join(os.path.expanduser('~'), '.danboorudata')
    db_filename = os.path.join(os.path.expanduser('~'), '.danboorudb')
    namepattern = re.compile(r'(?:\d+_)?([a-f\d]{32})')
    idpattern = re.compile(r'(\d+)_[a-f\d]{32}')
    logfile = 'error.log'

    def __init__(self, args, limit, offset, **kwargs):
        for key, value in kwargs.iteritems():
            self[key] = value
        self.end = lambda text, start: '%s (%.2fs)' % (text, time()-start)
        self.tags = self.parse_tags(args)
        self.settings = self.load_settings()
        #~ self.servers = self.load_servers()
        self.db, self.cur = self.load_db()
        self.folder = self.tags
        self.limit = limit
        self.offset = offset
        self.dl = Downloader()

    def get_last_id(self, pathname):
        '''Get the youngest file by its danbooru id'''
        if self['refresh']:
            return ''
        filenames = self.get_filenames(pathname)
        filenames = sorted(filenames)[::-1]
        for item in filenames:
            folder, name, ext = self.split_path(item)
            match = re.match(self.idpattern, name)
            if match:
                id = int(match.group(1).lstrip('0'))
                break
        else: id = 1
        return self.last_id % (int(id),)

    def error(self, message):
        print >> stderr, 'Error: %s' % (message,)

    def retrieve_content(self):
        '''Start downloading'''
        print 'Downloading to %s...' % (self.folder,)
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)
        last_id = self.get_last_id(self.folder)
        step, limit, offset = 100, self.limit, self.offset
        for i in xrange(offset, limit, step):
            j = i+step if i+step < limit else limit
            params = { 'tags': self.tags, 'last_id': last_id, 'limit': j,
                'offset': i, 'rating': self.rating_path % self['rating'] \
                    if self['rating'] else ''}
            path, start = self.posts_path % params, time()
            url = self.api_url+path
            print 'API:', path+'...',
            data = self.get_data(url, 'post', 'id')
            print self.end('done', start)
            if self['nodb'] or not len(data):
                print '%d posts returned' % (len(data),)
                if not len(data): break
            else:
                before = len(data)
                self.filter_data(data)
                values = (before, case(before, 'post'),
                    len(data), cases(len(data), 'wasn\'t', 'weren\'t'))
                print '%d %s returned, %d %s in the local database' % values
            if self['simulate']: continue
            if len(data):
                for key, value in data.iteritems():
                    self.get_post(key, value)
            self.update_db(data)
            self.db.commit()
            try:
                if before < step: break
            except UnboundLocalError, e:
                print e
        else:
            print 'Post limit (%d) met' % (limit,)
        if not glob(os.path.join(self.folder, '*')):
            print '%s is empty: removing' % (self.folder,)
            os.rmdir(self.folder)

    def get_post(self, id, post):
        '''Download an individual post'''
        print post
        file_url = post['file_url']
        filename = file_url[file_url.rfind('/')+1:]
        # Figure out the local name (id is padded with zeroes)
        localname = os.path.join(self.folder, '%07d_%s' % (id, filename))
        if os.path.exists(localname):
            self.error('File already exists')
            return
        #~ server = {'h
        server = 'http://s3.amazonaws.com/danbooru/'
        #~ url = server + '/'.join((filename[0:2], filename[2:4], filename))
        url = server + filename
        print url
        print self.dl.retrieve(url, localname, self.exit), \
            'KiB retrieved in %s' % (self.folder,)

    def filter_data(self, data):
        '''Filter out the data that already exists in the local db'''
        values = '","'.join([str(key) for key in data.keys()])
        query = self.cur.execute(self.by_id_command % values)
        for row in query:
            id, = row
            if id in data:
                del data[id]
        return data

    def log(self, message):
        '''Unused'''
        print >> open(self.logfile, 'a+'), message

    def use_server(self, id):
        '''Set the server for this instance to use'''
        #~ if id not in self.servers:
            #~ raise ServerIdError, id
        #~ self.server = 0

    def load_servers(self):
        '''Load the servers from stored data'''
        #~ if 'servers' in self.settings:
            #~ servers = self.settings['servers']
        #~ else:
            #~ servers = self.update_servers()
        #~ return servers

    # TODO: remove redundant parts and use the get_data() method instead
    def update_servers(self):
        '''Download and parse the server list from the api'''
        #~ if 'servers_fresh' in dir(self):
            #~ return self.servers
        #~ print 'Updating servers list...',
        #~ start = time()
        #~ url = self.api_url + self.servers_path
        #~ data = minidom.parse(urllib.urlopen(url))
        #~ results = {}
        #~ for server in data.getElementsByTagName('server'):
            #~ attributes = dict(server.attributes.items())
            #~ results[int(attributes.pop('id'))] = attributes
        #~ if not len(results):
            #~ print self.end('done', start)
            #~ self.error('danbooru seems to be down')
            #~ self.exit()
        #~ data.unlink()
        #~ self.servers_fresh = True
        #~ self.save_settings(servers=results)
        #~ print self.end('done', start)
        #~ return results

    def list_servers(self):
        '''Print servers'''
        #~ print 'Listing servers...'
        #~ row = lambda id, host: '%s %s' % (str(id).rjust(2), host)
        #~ print row('ID', 'Host')
        #~ for id in self.servers:
                #~ print row(id, self.servers[id]['host']), '[default]' \
                    #~ if id == self.settings['default'] else ''

    def load_settings(self):
        '''Connect to the persistent settings'''
        return shelve.open(self.settings_filename)

    def save_settings(self, **kwargs):
        '''Save and flush settings'''
        for key in kwargs:
            self.settings[key] = kwargs[key]
        self.settings.sync()

    def get_data(self, url, elementname, keyname):
        '''Fetch and parse data from the api (would be many lines longer if \
this had to be actually spidered)'''
        data = minidom.parse(urllib.urlopen(url))
        results = {}
        for server in data.getElementsByTagName(elementname):
            attributes = dict(server.attributes.items())
            results[int(attributes.pop(keyname))] = attributes
        data.unlink()
        return results

    def get_serverlist(self):
        '''asd'''
        #~ url, start = self.api_url + self.servers_path, time()
        #~ print 'Getting servers list...',
        #~ results = self.get_data(url, 'server', 'id')
        #~ print self.end(start)
        #~ return results

    def get_content_data(self, tags=None, limit=None, offset=None, hashes=None):
        if tags and limit and offset:
            url = self.api_url + self.posts_path % (tags, limit, offset)
            count = limit - offset
        elif hashes:
            url = self.api_url + self.md5_path % ','.join(hashes)
            count = len(hashes)
        print 'Getting content data for %d %s...' % (count, case(count, 'file')),
        start = time()
        results = self.get_data(url, 'post', 'id')
        print self.end('done', start)
        if len(results) < count:
            missing = count - len(results)
            print '%d %s not found' % (missing, case(missing, 'file'))
        return results

    def parse_tags(self, args):
        '''Parse script arguments'''
        tags = [urllib.quote(item.replace(' ', '_')).replace('%2B', '+') \
            for item in args]
        tags = '+'.join(tags)
        return tags

    def exit(self):
        '''Say bye and report db changes'''
        changes = self.db.total_changes
        if changes:
            print '%d %s to the local database in this session' % \
                (changes, case(changes, 'change'))
        print 'Bye~!'
        exit()

    def load_db(self):
        '''Connect to the sqlite db'''
        self.init_db_command ='''CREATE TABLE IF NOT EXISTS content \
(id INTEGER PRIMARY KEY, md5 TEXT, tags TEXT, misc BLOB);'''
        self.update_db_command ='''INSERT OR IGNORE into content \
(id, md5, tags, misc) values (%d, "%s", "%s", "%s");'''
        self.by_md5_command ='''SELECT md5, id FROM content \
WHERE md5 IN ("%s");'''
        self.by_id_command ='''SELECT id FROM content WHERE id IN ("%s");'''
        db = sqlite3.connect(self.db_filename)
        db.text_factory = lambda text: unicode(text, 'utf-8', 'ignore')
        cur = db.cursor()
        cur.execute(self.init_db_command)
        return db, cur

    def hash_in_filename(self, filename):
        '''Try to avoid hashing the file'''
        name, ext = os.path.splitext(os.path.basename(filename))
        results = re.search(self.namepattern, name)
        return results.groups()[0] if results else None

    def filter_hashes(self, hashes):
        '''Remove hashes that exist in the local database'''
        values = '","'.join(hashes.values())
        query = self.cur.execute(self.by_md5_command % values)
        for hash in query:
            hash, id = hash
            for key, value in hashes.copy().iteritems():
                if hash != value: continue
                del hashes[key]
        return hashes

    def get_hashes(self, names, source, filter=True):
        '''Get hashes for files in a path'''
        print 'Getting hashes for %d %s in %s...' % \
            (len(names), case(len(names), 'file'), source),
        results, start = {}, time()
        for item in names:
            hash = self.hash_in_filename(item) \
                or md5(open(item, 'rb').read()).hexdigest()
            results[item] = hash
        if filter:
            results = self.filter_hashes(results)
        print self.end('done', start)
        return results

    def catalogue_content(self, pathname):
        '''Add files to the local database'''
        print 'Starting to catalogue %s...' % (pathname,)
        filenames = self.get_filenames(pathname)
        hashes = self.get_hashes(filenames, pathname)
        message = '%d of %d files already in local database'
        print message % (len(filenames)-len(hashes), len(filenames))
        count, step = 0, 100
        for i in xrange(0, len(hashes), step):
            data = self.get_content_data(hashes=hashes.values()[i:i+step])
            count += len(data)
            if self['simulate']: continue
            self.update_db(data)
            self.db.commit()
        print '%d %s added to database' % (count, cases(count, 'entry', 'entries'))

    def split_path(self, pathname):
        '''Split the path in a tuple of three'''
        folder, name = os.path.split(pathname)
        name, ext = os.path.splitext(name)
        return folder, name, ext

    def fix_filenames(self, pathname):
        '''Rename files to id_hash'''
        print 'Fixing filenames in %s...' % (pathname,)
        filenames = self.get_filenames(pathname)
        start, count = time(), 0
        hashes = self.get_hashes(filenames, pathname, filter=False)
        values = '","'.join(hashes.values())
        query = self.cur.execute(self.by_md5_command % values)
        query = dict(query.fetchall())
        for filename, hash in hashes.iteritems():
            if hash not in query.keys():
                continue
            folder, oldname, ext = self.split_path(filename)
            # Figure out the new name (id is padded with zeroes)
            newname = '%07d_%s%s' % (query[hash], hash, ext)
            newname = os.path.join(folder, newname)
            if filename == newname:
                continue
            if os.path.exists(newname):
                os.remove(filename)
            else:
                try: os.rename(filename, newname)
                except WindowsError, e:
                    print e
                    count -= 1
                count += 1
        print '%d %s fixed' % (count, case(count, 'filename'))

    def expand_paths(self, source):
        '''Does exactly what the name says'''
        names = set()
        for item in source.split():
            names.update(glob(item))
        return filter(os.path.isdir, names)

    def get_filenames(self, pathname):
        '''Again, does just what the name says'''
        names = glob(os.path.join(pathname, '*'))
        return filter(os.path.isfile, names)

    def update_db(self, data):
        '''Write data to the transaction (has to be committed to the db explicitly)'''
        for key, value in data.iteritems():
            value['author'] = value['author'].encode('utf-8')
            values = (key, value.pop('md5'), value.pop('tags'), pickle.dumps(value))
            try:
                self.cur.execute(self.update_db_command % values)
            except sqlite3.OperationalError, e:
                print e


class Downloader(object):
    '''Shows a progress bar for downloads. this is actually useful outside the
    scope of danbooru.py'''

    before = .0
    history = []
    cycles = 0
    average = lambda self: sum(self.history) / (len(self.history) or 1)

    def __init__(self, width=55):
        self.width = width
        self.kibi = lambda bits: bits / 2 ** 10
        self.proc = lambda a, b: a / (b * 0.01)

    def retrieve(self, url, destination, callback=None):
        self.size = 0
        xtime()
        try: urllib.urlretrieve(url, destination, self.progress)
        except KeyboardInterrupt:
            print '\nDownload cancelled'
            for i in range(5):
                try:
                    os.remove(destination)
                    break
                except:
                    sleep(.1)
            else: raise
            if callback: callback()
            exit()
        print
        return self.size

    def progress(self, blocks, blocksize, filesize):
        self.cycles += 1
        bits = min(blocks*blocksize, filesize)
        done = self.proc(bits, filesize) if bits != filesize else 100
        bar = self.bar(done)
        if not self.cycles % 3 and bits != filesize:
            now = xtime()
            elapsed = now-self.before
            if elapsed:
                speed = self.kibi(blocksize * 3 / elapsed)
                self.history.append(speed)
                self.history = self.history[-4:]
            self.before = now
        average = round(sum(self.history[-4:]) / 4, 1)
        self.size = self.kibi(bits)
        print '\r[%s] %s KiB/s  ' % (bar, str(average)),

    def bar(self, done):
        span = self.width * done * 0.01
        offset = len(str(int(done))) - .99
        result = ('%d%%' % (done,)).center(self.width)
        return result.replace(' ', '-', int(span - offset))


def parse_options():
    '''Parse arguments passed to the script'''
    help = { 'limit': 'set how many posts (not files) to get from the api \
[default: %default]',
        'offset': 'set the position to start downloading from \
[default: %default]',
        'server': 'which server to use (takes an index, see -L for a list of \
available servers)',
        'refresh': 'allow retrieving posts older than the highest id \
of the local files in the destination folder',
        'nodb': 'allow downloading posts that are already present \
in the local database',
        'catalogue': 'add local files to the database \
(queries the api with their hashes)',
        'fixnames': 'change filenames to <id>_<hash>.* format',
        'folder': 'override the download destination (default is same as tags)',
        'update': 'update the serverlist',
        'list': 'see a list of available servers',
        'set_default': 'set a default server',
        'rating': 'convenience shortcut to the rating: tag',
        'simulate': 'don\'t download files or add posts to the database',
    }
    usage = '%prog [-l NUM] [-o NUM] [-s NUM] [-r safe|questionable|explicit] \
[-f PATH] [-i] [-n] [-c PATH] [-x PATH] [-u] [-L] [-d] <tags>'
    from optparse import OptionParser
    parser = OptionParser(usage=usage, version='%s.%s' % (__version__, __build__),
        description='A tool for retrieving content from danbooru.donmai.us')
    parser.add_option('-l', '--limit', dest='limit', help=help['limit'], \
        metavar='NUM', default=1000, type='int')
    parser.add_option('-o', '--offset', dest='offset', help=help['offset'], \
        metavar='NUM', default=0, type='int')
    #~ parser.add_option('-s', '--server', dest='server', help=help['server'], \
        #~ metavar='NUM', default=None, type='int')
    parser.add_option('-r', '--rating', dest='rating', help=help['rating'], \
        metavar='NAME', default=None, type='string')
    parser.add_option('-f', '--folder', dest='folder', help=help['folder'], \
        metavar='PATH', default=None)
    #~ parser.add_option('-i', '--no-last-id', dest='refresh', \
        #~ help=help['refresh'], action='store_true', default=False)
    parser.add_option('-n', '--no-db', dest='nodb', help=help['nodb'], \
        action='store_true', default=False)
    parser.add_option('-c', '--catalogue', dest='catalogue', \
        help=help['catalogue'], metavar='PATH', default=None)
    parser.add_option('-x', '--fix', dest='fixnames', help=help['fixnames'], \
        metavar='PATH', default=None)
    #~ parser.add_option('-u', '--update', dest='update', help=help['update'], \
        #~ action='store_true', default=False)
    #~ parser.add_option('-L', '--list', dest='list', help=help['list'], \
        #~ action='store_true', default=False)
    #~ parser.add_option('-d', '--default', dest='set_default', \
        #~ help=help['set_default'], metavar='ID', default=None, type='int')
    parser.add_option('-e', '--simulate', dest='simulate', \
        help=help['simulate'], action='store_true', default=False)
    options, args = parser.parse_args()
    return options, args, parser


def main():
    '''Decide what to do based on the options returned by optparse'''
    options, args, parser = parse_options()
    robot = Robot(args, options.limit, options.offset, rating=options.rating,
        refresh=False, nodb=options.nodb, simulate=options.simulate)
    if options.rating:
        values = ('safe', 'explicit', 'questionable')
        if options.rating not in values:
            parser.error('only %r are valid ratings' % (values,))
        else:
            robot.rating = options.rating
    #~ if options.refresh:
        #~ print 'Using No Last ID mode...'
    if options.folder:
        robot.folder = options.folder
    if options.catalogue:
        for name in robot.expand_paths(options.catalogue):
            robot.catalogue_content(name)
    if options.fixnames:
        for name in robot.expand_paths(options.fixnames):
            robot.fix_filenames(name)
    #~ if options.update:
        #~ robot.update_servers()
    #~ if options.set_default:
        #~ server_id = options.set_default
        #~ robot.use_server(server_id)
        #~ robot.save_settings(default=server_id)
        #~ print 'Default server set to %d (%s)' % \
            #~ (server_id, robot.servers[server_id]['host'])
    #~ if 'default' not in robot.settings:
        #~ server_id = robot.servers.keys()[0]
        #~ robot.use_server(server_id)
        #~ robot.save_settings(default=server_id)
    #~ if options.list:
        #~ robot.list_servers()
    #~ elif options.server:
        #~ robot.use_server(options.server)
    #~ else:
    #~ robot.use_server(robot.settings['default'])
    #~ if robot.tags:
        #~ if 'server' not in dir(robot):
            #~ robot.use_server(robot.settings['default'])
        #~ print 'Using server %d (%s)' % \
            #~ (robot.server, robot.servers[robot.server]['host'])
    robot.retrieve_content()
    robot.exit()


if __name__ == '__main__':
    main()
