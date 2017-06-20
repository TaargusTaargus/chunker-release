from StringIO import StringIO
from gzip import open as g_open
from utilities import ensure_path, persistent_try
from time import sleep
from os.path import join
from state import flags

class Storage:

  def __init__( self, client ):
    self.storage = flags[ 'storage_mode' ]
    self.client = client 

  def __download_from_box__( self, id ):
    return self.client.file( id ).content()


  def __download_from_drive__( self, id ):
    return self.client.CreateFile( { 'id': id } ).GetContentString()

  def __upload_to_box__( self, folder_id, chunk ):
    stream = StringIO()
    stream.write( chunk.string )
    stream.seek( 0 )
    box_file = self.client.folder( folder_id ).upload_stream( stream, chunk.chunk_name )
    return box_file.id


  def __upload_to_drive__( self, folder_id, chunk ):
    file = self.client.CreateFile( { 'title': chunk.chunk_name, 'parents': [{ 'id': folder_id }] } )
    file.SetContentString( chunk.string )
    file.Upload()
    return file[ 'id' ]

  def read_chunk( self, id ):
    if self.storage == 'box':
      return persistent_try( self.__download_from_box__, [ id ], 'downloading' )
    elif self.storage == 'drive':
      return persistent_try( self.__download_from_drive__, [ id ], 'downloading' )

  def write_chunk( self, chunk, wdir ):
    if self.storage == 'box':
      return persistent_try( self.__upload_to_box__, [ wdir, chunk ], 'uploading' )
    elif self.storage == 'drive':
      return persistent_try( self.__upload_to_drive__, [ wdir, chunk ], 'uploading' )
