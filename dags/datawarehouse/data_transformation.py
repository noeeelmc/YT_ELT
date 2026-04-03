from datetime import timedelta, datetime

def parse_duration(duration_str):
    
    """Duration is in format P#DT#H#M#S, where P indicates the period, D is days, T separates date and time, H is hours, M is minutes and S is seconds. 
    This function parses the duration string and returns a timedelta object."""
    
    duration_str = duration_str.replace("P", "").replace("T","")
    
    components = ["D","H","M","S"]
    values = {"D":0, "H":0, "M":0, "S":0}
    
    for component in components:
        if component in duration_str:
            value = duration_str.split(component)[0]
            values[component] = int(value)
            
    total_duration = timedelta(days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"])
    
    return total_duration

def transform_data(row):
    
    duration_td = parse_duration(row["Duration"])
    
    row["Duration"] = (datetime.min + duration_td).time()
    
    row["Video_Type"] = "Short" if duration_td.total_seconds() <= 60 else "Normal"
    
    return row
    