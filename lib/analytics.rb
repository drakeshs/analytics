# zxth analytics system
module Analytics
    class Util
        # get query time by hour method
        def Util.hour_query_time
            now = Time.new
            last_hour_time = now-(60*60)
            last_hour_start = Time.mktime(last_hour_time.year,last_hour_time.month,last_hour_time.day,last_hour_time.hour,0,0).to_i
            last_hour_end = last_hour_start+(60*60)-1
            return now.to_i*1000,last_hour_start*1000,last_hour_end*1000
        end
        # get query time by hour method
        def Util.day_query_time
            now = Time.new
            last_day_time = now-(60*60*24)
            last_day_start = Time.mktime(last_day_time.year,last_day_time.month,last_day_time.day,0,0,0).to_i
            last_day_end = last_day_start+(60*60*24)-1
            return now.to_i*1000,last_day_start*1000,last_day_end*1000
        end
    end
    #Fetch main visitor data
    class FetchMainGatherData
        def initialize(site_id,client)
            @site_id = site_id
            @client = client
        end
        # gather visitor day data
        def gatherDayVisitor
            pv = client.query("select count(id) from visitors where site_id=#{site_id} and created_at > #{yesterday_start} and created_at < #{yesterday_end}",:as=>:array).first.first
            ipv = client.query("select count(distinct ip) from visitors where site_id=#{site_id} and created_at > #{yesterday_start} and created_at < #{yesterday_end}").first.first
            @client.query("insert into site_day_gather(site_id,pv,ipv,day_time,created_at) 
                 values( #{site_id},
                         #{pv},
                         #{ipv},
                         #{yesterday_start},
                         #{now})")
        end
        # gather visitor hour data
        def gatherHourVisitor
            pv = client.query("select count(id) from visitors where site_id=#{site_id} and created_at >= #{last_hour_start} and created_at <= #{last_hour_end}",:as=>:array).first.first
            ipv = client.query("select count(distinct ip) from visitors where site_id=#{site_id} and created_at >= #{last_hour_start} and created_at <= #{last_hour_end}").first.first
            @client.query("insert into site_hour_gather(site_id,pv,ipv,hour_time,created_at) " +
                 "values( #{site_id},
                         #{pv},
                         #{ipv},
                         #{last_hour_start},
                         #{now})")
        end
        # gather dictionary day visitor data
        def gatherDicDayVisitor
        end
    end
end
