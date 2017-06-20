from StringIO import StringIO
from gzip import open as g_open
from utilities import trim, persistent_try
from time import sleep
from os import makedirs
from os.path import join
from state import flags

class FilePair:

  def __init__( self, src, dest ):
    self.fsource = src
    self.fdest = dest


class Filesystem:

  def __init__( self, client, root, mode='drive' ):
    self.client = client
    self.mode = mode
    self.root = root
    self.dirs = { root : self.__mkdir__( root ) }


  def __box_mkdir__( self, abs_pth ):
    abs_pth = trim( abs_pth ) 
    root = "0"
    for rel_path in abs_pth.split( "/" ):
      flag = False
      for e in self.client.folder( root ).get_items( 10000 ):
        if e.name == rel_path:
          flag = True
          root = e.id
    return root


  def __drive_mkdir__( self, abs_pth ):
    abs_pth = trim( abs_pth )
    root = "root"
    for rel_path in abs_pth.split( "/" ):
      flag = False;
      for e in self.client.ListFile({'q': "'" + root + "' in parents"}).GetList():
        if e[ 'title' ] == rel_path:
          flag = True
          root = e[ 'id' ]
      if not flag:
        file = self.client.CreateFile( { 'title': rel_path, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [ { 'id': root } ] } )
        file.Upload()
        root = file[ 'id' ]
    return root		


  def __mkdir__( self, folder_path ):
    if self.mode == 'box':
      return persistent_try( self.__box_mkdir__, [ folder_path ], 'resolving folder' )
    elif self.mode == 'drive':
      return persistent_try( self.__drive_mkdir__, [ folder_path ], 'resolving folder' )


  def mkdir( self, relpath, rid=None ):
    if rid:
      self.dirs[ relpath ] = rid 
  
    if relpath not in self.dirs:
      self.dirs[ relpath ] = self.__mkdir__( join( self.root, relpath ) )


  def get_filepair( self, key=None ):
    try: 
      return FilePair( key, self.dirs[ key ] )
    except:
      return FilePair( key, self.dirs[ self.root ] )
