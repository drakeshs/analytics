require 'test/unit'

class TestAnalytics < Test::Unit::TestCase
  should "not null" do
      Analytics::Util.day_query_time
      Analytics::Util.hour_query_time
      #connect to mysql
      DB='analytics'
      client = Mysql2::Client.new(:host => "localhost", :username => "root",
                                  :socket => '/tmp/mysql.sock',:encoding => 'utf8',:database => DB)
      fetcher = Analytics::FetchMainGatherData.new(1,client)
      fetcher.gatherDayVisitor
      fetcher.gatherHourVisitor
  end
end
