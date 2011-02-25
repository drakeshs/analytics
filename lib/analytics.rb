# zxth analytics system
require 'mysql'

module Analytics
    #connect to mysql
    DB='analytics'
    @client = nil
    @stmt = nil 
    def self.init
        @client=Mysql.new("localhost", "root",nil,DB,nil,'/tmp/mysql.sock')
        @client.autocommit(true)
        @stmt = @client.stmt_init
    end
    def self.close
        @stmt.close
        @client.close
    end
    def self.client
        @client
    end
    def self.stmt
        @stmt
    end
    # gather day visitor data
    def self.gather_day_visitor
        Util.gather_data{|site_id|
            gatherer = FetchMainGatherData.new(site_id)
            gatherer.gatherDayVisitor
        }
    end
    # gather hour visitor data
    def self.gather_hour_visitor
        Util.gather_data{|site_id|
            gatherer = FetchMainGatherData.new(site_id)
            gatherer.gatherHourVisitor
        }
    end
    #gather visitor data using dictionary group
    def self.gather_dic_visitor gather_table,column_name
        Util.gather_data{|site_id|
            gatherer = FetchMainGatherData.new(site_id)
            gatherer.gatherDicDayVisitor gather_table,column_name
        }
    end
    class Util
        def self.gather_data
            #Fetch all sites
            sites = []
            Analytics.client.query("select id from sites").each{|r| sites << r[0]}
            #Gather day visitor data
            sites.each{|site_id|
                begin
                    yield(site_id)
                rescue => err
                    puts err
                    # TODO 自动发送错误信息
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
        def initialize(site_id)
            @site_id = site_id
            @client = Analytics.client
            @stmt = Analytics.stmt
        end
        # gather visitor day data
        def gatherDayVisitor
            now,yesterday_start,yesterday_end = Util.day_query_time
            pv = @client.query("select count(id) from visitors where site_id=#{@site_id} and created_at > #{yesterday_start} and created_at < #{yesterday_end}").fetch_row[0]
            ipv = @client.query("select count(distinct ip_id) from visitors where site_id=#{@site_id} and created_at > #{yesterday_start} and created_at < #{yesterday_end}").fetch_row[0]
            r = @client.query("select count(id) from site_day_gather where site_id=#{@site_id} and day_time=#{yesterday_start}").fetch_row[0]
            if(r == 0)
                @stmt.prepare("insert into site_day_gather(site_id,pv,ipv,day_time,created_at) "+
                                " values(?,?,?,?,?)" )
                @stmt.execute(@site_id,pv,ipv,yesterday_start,now)
            else
                @stmt.prepare("update site_day_gather set pv=?,ipv=?,created_at=? where site_id=? and day_time=?")
                @stmt.execute(pv,ipv,now,@site_id,yesterday_start)
            end
        end
        # gather visitor hour data
        def gatherHourVisitor
            now,last_hour_start,last_hour_end = Util.hour_query_time
            pv = @client.query("select count(id) from visitors where site_id=#{@site_id} and created_at >= #{last_hour_start} and created_at <= #{last_hour_end}").fetch_row[0]
            ipv = @client.query("select count(distinct ip_id) from visitors where site_id=#{@site_id} and created_at >= #{last_hour_start} and created_at <= #{last_hour_end}").fetch_row[0]

            r = @client.query("select count(id) from site_hour_gather where site_id=#{@site_id} and hour_time=#{last_hour_start}").fetch_row[0]
            if(r == 0)
                @stmt.prepare("insert into site_hour_gather(site_id,pv,ipv,hour_time,created_at) "+
                                " values(?,?,?,?,?)" )
                @stmt.execute(@site_id,pv,ipv,last_hour_start,now)
            else
                @stmt.prepare("update site_hour_gather set pv=?,ipv=?,created_at=? where site_id=? and hour_time=?")
                @stmt.execute(pv,ipv,now,@site_id,last_hour_start)
            end
        end
        # gather dictionary day visitor data
        def gatherDicDayVisitor gather_table,col

            #get query timestamp
            now,yesterday_start,yesterday_end = Util.day_query_time

            @client.query("select count(id),#{col} from visitors where site_id=#{@site_id} and created_at > #{yesterday_start} and created_at < #{yesterday_end} group by #{col}").each{|row|


            r = @client.query("select count(id) from #{gather_table} where site_id=#{@site_id} and day_time=#{yesterday_start} and #{col}=#{row[1]}").fetch_row[0]
            if(r == 0 )
                @stmt.prepare("insert into #{gather_table}(site_id,pv,day_time,created_at,#{col}) "+
                              " values(?,?,?,?,?)" )
                @stmt.execute(@site_id,row[0],yesterday_start,now,row[1])
            else
                @stmt.prepare("update #{gather_table} set pv=?,created_at=? where site_id=? and day_time=? and #{col}=?")
                @stmt.execute(row[0],now,@site_id,yesterday_start,row[1])
            end
            }
        end
    end
end
