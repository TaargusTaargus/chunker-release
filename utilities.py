from time import sleep
from os import makedirs, sep
from os.path import isdir
from os.path import relpath as os_relpath
from state import flags

def dwalk( client, root ):
  dirs = [ ( root, "" ) ]
  while dirs:
    dir = dirs.pop()
    cdirs, files = lsplit( client.ListFile({'q': "'" + dir[ 0 ] + "' in parents"}).GetList(), 'mimeType', 'application/vnd.google-apps.folder' )
    dirs.extend( [ ( e[ 'id' ], dir[ 1 ] + e[ 'title' ] + sep ) for e in cdirs ] )
    yield dir[ 1 ], dir[ 0 ], files, cdirs



def lsplit( list, key, val ):
  w, wo = [], []
  while list:
    el = list.pop()
    if( el[ key ] == val ):
      w.append( el )
    else:
      wo.append( el )
  return w, wo


def relpath( path1, path2 ):
  path = path1 if path2 == '' else os_relpath( path1, path2 )
  return path if path != '.' else ''


def ensure_path( path ):
  if not isdir( path ):
    makedirs( path )


def trim( path ):
  path = path[ 1: ] if path[ 0 ] == '/' else path
  return path[ :-1 ] if path[ -1 ] == '/' else path


def persistent_try( function, args, description ):
  count = 10
  while True:
    try:
      return function( *args )
    except Exception as e:
      if count == 0:
        print( "ERROR: hit retry limit " + description + " ... " )
        print( "REASON: " + str( e ) )
        return None
      else:
        print( 'WARNING: encountered issue ' + description
              + ' with ' + flags[ 'storage_mode' ] + ', will retry ...' )
      sleep( 2 )

    count = count - 1
