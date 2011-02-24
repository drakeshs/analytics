require 'rubygems'
require 'analytics'
Analytics.init
0..100.times {|i|
    Analytics.gather_hour_visitor
    Analytics.gather_day_visitor
    Analytics.gather_dic_visitor "site_day_browser_gather","browser_id"
}
Analytics.close
