# zxth analytics system
require 'mysql2'

module Analytics
    #connect to mysql
    DB='analytics'
    CLIENT= Mysql2::Client.new(:host => "localhost", :username => "root",
                                :socket => '/tmp/mysql.sock',:encoding => 'utf8',
                                :database => DB)
    # gather day visitor data
    def self.gather_day_visitor
        Util.gather_data{|site_id,client|
            gatherer = FetchMainGatherData.new(site_id,client)
            gatherer.gatherDayVisitor
        }
    end
    # gather hour visitor data
    def self.gather_hour_visitor
        Util.gather_data{|site_id,client|
            gatherer = FetchMainGatherData.new(site_id,client)
            gatherer.gatherHourVisitor
        }
    end
    #gather visitor data using dictionary group
    def self.gather_dic_visitor gather_table,column_name
        Util.gather_data{|site_id,client|
            gatherer = FetchMainGatherData.new(site_id,client)
            gatherer.gatherDicDayVisitor gather_table,column_name
        }
    end
    class Util
        def self.gather_data
            #Fetch all sites
            sites = []
            CLIENT.query("select id from sites",:as=> :array).each{|r| sites << r[0]}
            #Gather day visitor data
            sites.each{|site_id|
                begin
                    yield(site_id,CLIENT)
                rescue
                    # 自动发送错误信息
                end
            }
        end
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
            now,yesterday_start,yesterday_end = Util.day_query_time
            pv = @client.query("select count(id) from visitors where site_id=#{@site_id} and created_at > #{yesterday_start} and created_at < #{yesterday_end}",:as=>:array).first.first
            ipv = @client.query("select count(distinct ip) from visitors where site_id=#{@site_id} and created_at > #{yesterday_start} and created_at < #{yesterday_end}").first.first
            @client.query("insert into site_day_gather(site_id,pv,ipv,day_time,created_at) 
                 values( #{@site_id},
                         #{pv},
                         #{ipv},
                         #{yesterday_start},
                         #{now})")
        end
        # gather visitor hour data
        def gatherHourVisitor
            now,last_hour_start,last_hour_end = Util.hour_query_time
            pv = @client.query("select count(id) from visitors where site_id=#{@site_id} and created_at >= #{last_hour_start} and created_at <= #{last_hour_end}",:as=>:array).first.first
            ipv = @client.query("select count(distinct ip) from visitors where site_id=#{@site_id} and created_at >= #{last_hour_start} and created_at <= #{last_hour_end}").first.first
            @client.query("insert into site_hour_gather(site_id,pv,ipv,hour_time,created_at) " +
                 "values( #{@site_id},
                         #{pv},
                         #{ipv},
                         #{last_hour_start},
                         #{now})")
        end
        # gather dictionary day visitor data
        def gatherDicDayVisitor gather_table,col

            #get query timestamp
            now,yesterday_start,yesterday_end = Util.day_query_time

            @client.query("select count(id),#{col} from visitors where site_id=#{@site_id} and created_at > #{yesterday_start} and created_at < #{yesterday_end} group by #{col}",:as=>:array).each{|row|

                @client.query("insert into #{gather_table}(site_id,pv,#{col},day_time,created_at) 
                 values( #{@site_id},
                              #{row[0]},
                              #{row[1]},
                              #{yesterday_start},
                              #{now})")
            }
        end
    end
end
