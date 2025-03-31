
# Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date

# Initialize Spark Session
spark = SparkSession.builder.appName("Find Earliest Organizer").getOrCreate()

# Path of Json File
file_path = "file:///home/sniper/Downloads/Activity.json"


# Convert Json to DataFrame and Execute find Earliest Decision Maker using SparkSQL
def earliest_decision_maker(file_path):
    df=spark.read.format("json").option("header",True).option("multiline",True).load(file_path)
    organizer_df=df.select("organizers").withColumn("organiser",explode(col("organizers"))).select("organiser.*")
    activity_df=df.select("activities").withColumn("activity",explode(col("activities"))).select(col("activity.*")).withColumn("attendee_email",explode(col("attendees"))).withColumn("date",to_date(col("date"),"yyyy-MM-dd")).drop(col("attendees"))
    attendees_df=df.select("attendees").withColumn("attendee",explode(col("attendees"))).select("attendee.*")
    
    views=[(organizer_df,"organizer"),(activity_df,"activity"),(attendees_df,"attendee")]
    for d,view_name in views:
        d.createOrReplaceTempView(view_name)
    
    query=""" WITH CTE AS (SELECT MIN(a.date) AS earliest_date FROM activity a)
            SELECT o.email AS organizer_email ,o.first_name,o.last_name FROM organizer o 
            INNER JOIN activity a ON o.email=a.organizer 
            INNER JOIN attendee at ON a.attendee_email=at.email 
            WHERE at.role='Decision Maker' AND a.date= (SELECT earliest_date FROM CTE)
        """

    return spark.sql(query).show()


output=earliest_decision_maker(file_path)



