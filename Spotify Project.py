import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node track
track_node1712960080505 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-project-abhiramdesai/staging/track.csv"], "recurse": True}, transformation_ctx="track_node1712960080505")

# Script generated for node artist
artist_node1712960081878 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-project-abhiramdesai/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1712960081878")

# Script generated for node albums
albums_node1712960082529 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-project-abhiramdesai/staging/albums.csv"], "recurse": True}, transformation_ctx="albums_node1712960082529")

# Script generated for node Join Album & Artist
JoinAlbumArtist_node1712960270081 = Join.apply(frame1=artist_node1712960081878, frame2=albums_node1712960082529, keys1=["id"], keys2=["artist_id"], transformation_ctx="JoinAlbumArtist_node1712960270081")

# Script generated for node Join with Tracks
JoinwithTracks_node1712960391671 = Join.apply(frame1=JoinAlbumArtist_node1712960270081, frame2=track_node1712960080505, keys1=["track_id"], keys2=["track_id"], transformation_ctx="JoinwithTracks_node1712960391671")

# Script generated for node Drop Fields
DropFields_node1712960683316 = DropFields.apply(frame=JoinwithTracks_node1712960391671, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1712960683316")

# Script generated for node Destination
Destination_node1712960741542 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1712960683316, connection_type="s3", format="glueparquet", connection_options={"path": "s3://spotify-project-abhiramdesai/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1712960741542")

job.commit()