import logging 

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur, conn, schema, row):
    
    try: 
        if schema == "staging":
            
            video_id = "video_id"
            
            cur.execute(
                f"""INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES(%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
                """, row
            )
                
        else:
            
            video_id = "Video_ID"
            
            cur.execute(
                f"""INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES(%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s);
                """, row
            ) 
            
        conn.commit()
        
        logger.info(f"Inserted row with video ID: {row[video_id]}")
        
    except Exception as e:
        logger.error(f"Error inserting row with video ID: {row[video_id]}")
        raise e
    

def update_rows(cur,conn,schema,row):
    """For updating changing columns in the dimension table, in this case the video title, views, likes and comments count"""
    try: 
        if schema == "staging":
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET "Video_Title" = %(title)s,
                    "Video_Views" = %(viewCount)s,
                    "Likes_Count" = %(likeCount)s,
                    "Comments_Count" = %(commentCount)s
                WHERE "Video_ID" = %(video_id)s;
                """, row
            )
            video_id="video_id"
        
        else: 
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET "Video_Title" = %(Video_Title)s,
                    "Video_Views" = %(Video_Views)s,
                    "Likes_Count" = %(Likes_Count)s,
                    "Comments_Count" = %(Comments_Count)s,
                    "Video_Type" = %(Video_Type)s
                WHERE "Video_ID" = %(Video_ID)s;
                """, row
            )
            video_id="Video_ID"
            
        conn.commit()
            
        logger.info(f"Updated row with video ID: {row[video_id]}")
    
    except Exception as e:
        logger.error(f"Error updating row with video ID: {row.get(video_id, 'unknown')} - {e}")
        raise e

def delete_rows(cur, conn, schema, ids_to_delete):
    
    try:
        
        ids_to_delete = ", ".join(f"'{video_id}'" for video_id in ids_to_delete)
        
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN ({ids_to_delete});
            """
        )
        
        conn.commit()            
        logger.info(f"Deleted rows with video IDs: {ids_to_delete}")
    
    except Exception as e:
        logger.info(f"Error deleting rows with video IDs: {ids_to_delete} - {e}")
        raise e

    
    