# -*- coding: utf-8 -*-

"""Calculates the current version number.

If possible, uses output of “git describe” modified to conform to the
visioning scheme that setuptools uses (see PEP 386).  Releases must be
labelled with annotated tags (signed tags are annotated) of the following
format:

   v<num>(.<num>)+ [ {a|b|c|rc} <num> (.<num>)* ]

If “git describe” returns an error (likely because we're in an unpacked copy
of a release tarball, rather than a git working copy), or returns a tag that
does not match the above format, version is read from RELEASE-VERSION file.

To use this script, simply import it your setup.py file, and use the results
of getVersion() as your package version:

    import version
    setup(
        version=version.getVersion(),
        .
        .
        .
    )

This will automatically update the RELEASE-VERSION file.  The RELEASE-VERSION
file should *not* be checked into git but it *should* be included in sdist
tarballs (as should version.py file).  To do this, run:

    echo include RELEASE-VERSION version.py >>MANIFEST.in
    echo RELEASE-VERSION >>.gitignore

With that setup, a new release can be labelled by simply invoking:

    git tag -s v1.0
"""

__author__ = ('Douglas Creager <dcreager@dcreager.net>',
              'Michal Nazarewicz <mina86@mina86.com>')
__license__ = 'This file is placed into the public domain.'
__maintainer__ = 'Michal Nazarewicz'
__email__ = 'mina86@mina86.com'

__all__ = ('getVersion')


import re
import subprocess
import sys


RELEASE_VERSION_FILE = 'RELEASE-VERSION'

# https://pypi.python.org/pypi/Versio/0.1.2
_PEP440_VERSION_RE = r"""(?x)
                        ^
                        (\d[\.\d]*(?<= \d))
                        ((?:[abc]|rc)\d+)?
                        (?:(\.post\d+))?
                        (?:(\.dev\d+))?
                        $
                        """
_GIT_DESCRIPTION_RE = r'(?x)^v(?P<ver>%s)-(?P<commits>\d+)-g(?P<sha>[\da-f]+)$' % (
    _PEP440_VERSION_RE[4:].strip()[1:-1])


def readGitVersion():
    try:
        proc = subprocess.Popen(('git', 'describe', '--long',
                                 '--match', 'v[0-9]*.*'),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        data, _ = proc.communicate()
        print(data)
        if proc.returncode:
            return None
        ver = data.splitlines()[0].strip()
    except:
        return None

    if not ver:
        return None
    m = re.search(_GIT_DESCRIPTION_RE, ver.decode())
    if not m:
        sys.stderr.write('version: git description (%s) is invalid, '
                         'ignoring\n' % ver)
        return None

    commits = int(m.group('commits'))
    if not commits:
        return m.group('ver')
    else:
        return '%s.dev%d' % (
            m.group('ver'), commits)


def readReleaseVersion():
    try:
        fd = open(RELEASE_VERSION_FILE)
        try:
            ver = fd.readline().strip()
        finally:
            fd.close()
        if not re.search(_PEP440_VERSION_RE, ver):
            sys.stderr.write('version: release version (%s) is invalid, '
                             'will use it anyway\n' % ver)
        return ver
    except:
        return None


def writeReleaseVersion(version):
    fd = open(RELEASE_VERSION_FILE, 'w')
    fd.write('%s\n' % version)
    fd.close()


def getVersion():
    release_version = readReleaseVersion()
    version = readGitVersion() or release_version
    if not version:
        raise ValueError('Cannot find the version number')
    if version != release_version:
        writeReleaseVersion(version)
    return version


if __name__ == '__main__':
    print(getVersion())
