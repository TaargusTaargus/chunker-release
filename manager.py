from multiprocessing import Process, Queue
from os.path import join, abspath, isfile, isdir, relpath, normpath, basename
from os import walk, remove
from db import ChunkDB
from storage import Storage
from chunk import Chunker, Unchunker
from state import flags
from fs import Filesystem
from utilities import dwalk, ensure_path
from json import loads

class Manager( object ):

  def __init__( self, total_procs ):
    self.total = total_procs
    self.counter = 0
    self.init()
  
  def init( self ):
    self.proc_list = [ None ] * self.total
    for np in range( self.total ):
      self.proc_list[ np ] = self.__init_proc__( np )


  def __init_proc__( self, proc_num ):
    return None


  def __finish__( self, proc ):
    return None


  def next( self ):
    ret = self.proc_list[ self.counter ]
    self.counter = ( self.counter + 1 ) % self.total
    return ret


  def run( self ):
    try:
      for proc in self.proc_list:
        proc.start()
    
      while self.proc_list:
        proc = self.proc_list.pop( 0 )
        if not proc.is_alive():
          self.__finish__( proc )
        else:
          self.proc_list.append( proc ) 
    except:
      print( "error! cleaning up ..." )
      self.__finish__()


class UploadManager( Manager ):
  
  def __init__( self, credentials, db_name, total_procs=1 ):
    self.cred = credentials
    self.db = ChunkDB( db_name )
    self.fs = None
    Manager.__init__( self, total_procs )


  def __init_proc__( self, np ):
    p_db = ChunkDB( self.db.db_name + str( np ) )
    storage = Storage( self.cred.get_client() )
    return Chunker( storage, p_db )

  
  def __load__( self, file_name, abs_path, fpair ):
    try:
      entry = self.db.fill_dicts_from_db( [ '*' ], { "file_handle": join( fpair.fsource, file_name ) }, ChunkDB.FILE_TABLE, entries=1 ).pop()
    except:
      entry = None
    self.next().queue( file_name, abs_path, fpair, entry )

 
  def __finish__( self, proc ):
    self.db.copy_other_db( proc.meta_db )
    remove( proc.meta_db.db_name )


  def upload( self, read_path, write_dir ):
    #find all files within a directory, ignoring any existing chunk or metadata files
    all_files = []


    ### NEW CHALLENGES IS TO REVERT EVERYTHING BACK TO A CHUNKING FILE SYSTEM... TO AVOID ALL OF THIS FS
    client = self.cred.get_client()
    self.fs = Filesystem( client, basename( normpath( read_path ) ) )
    read_path = abspath( read_path )

    if isfile( read_path ):
      print( "ERROR: will only operate on directories ..." )
      return
    elif not isdir( read_path ):
      print( "ERROR: " + read_path + " does not exist ..." )
      return
    else:
      for root, dirs, files in walk( read_path, topdown=False, followlinks=False ):
        rel = relpath( root, read_path )
        rel = rel if rel[ 0 ] != '.' or len( rel ) > 1 else rel[ 1: ]

        if not flags[ 'collapse_flag' ] and rel:
          self.fs.mkdir( rel )
          #for dir in dirs:
          #  self.__load__( dir, root, self.fs.get_filepair( rel ) )

        for file in files + dirs:
          self.__load__( file, root, self.fs.get_filepair( rel ) )

    self.run() 



class DownloadManager( Manager ):
  
  def __init__( self, credentials, db_name=None, total_procs=1 ):
    self.cred = credentials
    self.db = ChunkDB( db_name ) if db_name else None
    self.fs = None
    Manager.__init__( self, total_procs )


  def __init_proc__( self, np ):
    storage = Storage( self.cred.get_client() )
    return Unchunker( self.db, storage )

   
  def __load_chunk__( self, write_dir, chunkid ):
    try:
      entry = self.db.fill_dicts_from_db( [ 'encoding', 'hash_key', 'init_vec' ], { "chunk_id": chunkid }, ChunkDB.CHUNK_TABLE, entries=1 ).pop()
      file_entries = self.db.get_chunk_file_entries( chunkid )
    except:
      entry = None
      file_entries = None
    self.next().queue_chunk( chunkid, write_dir, entry, file_entries )

  def __load_dir__( self, dbentry, wdir ):
    self.next().queue_dir( join( wdir, dbentry[ 'file_handle' ] ), dbentry )

  def __load_link__( self, dbentry, wdir ):
    self.next().queue_link( join( wdir, dbentry[ 'link_handle' ] ), dbentry[ 'link_dest' ] )
    

## download that does not require a db

#  def download( self, read_path, write_dir=None ):
#    #find all files within a directory, ignoring any existing chunk or metadata files
#    fs = Filesystem( self.cred.get_client(), read_path )
#    write_dir = write_dir if write_dir else read_path
#    all_files = []
#    ensure_path( abspath( write_dir ) )
#  
#		### filesystem only handles remote now
#   ### download does not write to database 
#   for path, id, files, dirs in dwalk( self.cred.get_client(), fs.dirs[ read_path ]  ):
#      abs_path = join( write_dir, path )   
 
#      if not flags[ 'collapse_flag' ] and path:
#        ensure_path( abs_path )
   
#      for file in files:
#        self.__load_chunk__( write_dir, abs_path, file ) 
       
#    self.run()

  def download( self, write_dir, download_path=None ):
    #find all files within a directory, ignoring any existing chunk or metadata files
    write_dir = write_dir if write_dir else read_path
    all_files = []
    ensure_path( abspath( write_dir ) )
 
    if not flags[ 'collapse_flag' ]:
      for entry in self.db.fill_dicts_from_db( [ 'directory_handle' ], None, ChunkDB.DIRECTORY_TABLE ):
        ensure_path( join( write_dir, entry[ 'directory_handle' ] ) )

    for entry in self.db.fill_dicts_from_db( [ 'chunk_id' ], None, ChunkDB.CHUNK_TABLE, distinct=True ):
      self.__load_chunk__( write_dir, entry[ 'chunk_id' ] )

    for entry in self.db.fill_dicts_from_db( [ '*' ], None, ChunkDB.SYMLINK_TABLE ):
      self.__load_link__( entry, write_dir )

    self.run()
    self.init()
    
    keys, entries = self.db.get_all_directory_permissions()
    for entry in entries:
      self.__load_dir__( dict( zip( keys, entry ) ), write_dir )
    self.run()

