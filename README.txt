dmedia - The Distributed Media Library
======================================

dmedia is simple distributed media library.  Media files are given a globally
unique ID based on their content-hash.  Meta-data is stored in CouchDB.
Meta-data for a large number of media files can be available locally (meta-data
is small).  Actual media files can be loaded on-demand from peers or cloud when
not available locally (media files are big).  Goals are to make synchronization
super easy, and to lay groundwork for a distributed content creation workflow.

Work on dmedia is coordinated through Launchpad.  For the latest code, to file a
bug, or to otherwise get involved, see the dmedia project in Launchpad:

    https://launchpad.net/dmedia

Although it didn't start as such, dmedia has become a foundational component for
the Novacut distributed video editor.  To learn more, see:

    http://novacut.com/

    https://launchpad.net/novacut

    https://launchpad.net/~novacut-community



Use and installation
====================

You can run the `dmedia` script directly from the source tree without any build
or installation step.  This is the quickest way to try dmedia.  You can run the
`dmedia` script from the source tree like this:

    ./dmedia /media/EOS_DIGITAL jpg cr2

That will recursively import all JPG and CR2 files from your memory card mounted
at "/media/EOS_DIGITAL".

To install dmedia system-wide, run this:

    sudo python setup.py install

Then you can run the `dmedia` script from any location like this:

    dmedia /media/EOS_DIGITAL jpg cr2

You can also install dmedia locally for your user only like this:

    python setup.py install --user

This will install dmedia in ~/.local/.  To run the dmedia script, you will have
to ensure that ~/.local/bin is in your path.  To do this, add the following in
your ~/.profile:

    # Also add .local/bin if it exists
    if [ -d "$HOME/.local/bin" ] ; then
        PATH="$HOME/.local/bin:$PATH"
    fi

You will have to logout and login for the change to take effect.



Testing the dmedia script
=========================

dmedia is at an early state still, but the functionality it has so far should
be quite stable.

The `dmedia` script will eventually be turned into a VCS-style script with
several commands.  For now it has a single function, to recursively import media
files from a directory.  At this point, it's a quick-and-dirty demo of how media
files might be stored and how their meta-data might be stored.

For example, say we scan all the JPG images in the '/usr/share/backgrounds'
directory:

    dmedia /usr/share/backgrounds jpg

Media files are uniquely ID'ed by their base32-encoded sha1 content-hash.  In
the above example, the 'Life_by_Paco_Espinoza.jpg' file happens to have a sha1
content-hash of '6BRRXCGRM2GKVPTREJPGRNGUR2GF2L4K'.  As such, this file
will be stored at:

    ~/.dmedia/6B/RRXCGRM2GKVPTREJPGRNGUR2GF2L4K.jpg

Meta-data for the media files is stored in CouchCB using desktop-couch.  Each
media file has its own document.  The sha1 content-hash is used as the document
'_id'.  For example, the 'Life_by_Paco_Espinoza.jpg' file has a document that
looks like this:

    {
       "_id": "6BRRXCGRM2GKVPTREJPGRNGUR2GF2L4K",
       "_rev": "1-c19ea015eb53ede147d63d55f3967d13",
       "name": "Life_by_Paco_Espinoza.jpg",
       "record_type": "http://example.com/dmedia",
       "bytes": 360889,
       "height": 1500,
       "shutter": "1/400",
       "width": 2000,
       "ext": "jpg",
       "camera": "DSC-H5",
       "iso": 125,
       "focal_length": "6.0 mm",
       "mtime": 1284394022,
       "aperture": 4
    }

All media files will have the following fields:

    bytes - File size in bytes
    mtime - Value of path.getmtime() at time of import
    name - The path.basename() part of the original source file
    ext - The extension of the original source file, normalized to lower-case

Additional fields depend upon the type of media file.  For example, image and
video files will always have 'width' and 'height', whereas video and audio files
will always have a 'duration'.

You can browse through the dmedia database using a standard web-browser, like
this:

    firefox ~/.local/share/desktop-couch/couchdb.html

Note that the sha1 hash is only being used as a stop-gap.  dmedia will use the
Skein hash after its final constant tweaks are made.  See:

    http://blog.novacut.com/2010/09/how-about-that-skein-hash.html
