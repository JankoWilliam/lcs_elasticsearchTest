import cn.yintech.online.LiveVisitOnline.jsonParse
import net.minidev.json.{JSONArray, JSONObject}
import net.minidev.json.parser.JSONParser



object Test2 {
  def main(args: Array[String]): Unit = {
    val str =
      """
        |{
        |	"code": 0,
        |	"msg": "成功",
        |	"data": {
        |		"banner": [{
        |			"id": "2516",
        |			"type": "viewdetail",
        |			"title": "翻身七月，抱“团”擒牛",
        |			"relation_id": "845064",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200708\/2020070809023063d602c48179d8fcb7afb8b160463939f763.jpeg",
        |			"url": "845064",
        |			"group_id": "1435",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"c_time": "2020-07-09 09:12:07",
        |			"route": {
        |				"type": "viewdetail",
        |				"relation_id": "845064",
        |				"url": "845064",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}, {
        |			"id": "2200",
        |			"type": "link",
        |			"title": "超值福利",
        |			"relation_id": "https:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0603?type=2&bid=10231",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200604\/20200604034329845aa72cc0f2c6b5ade8868b97b42bf6d784.png",
        |			"url": "https:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0603?type=2&bid=10231",
        |			"group_id": "1272",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"c_time": "2020-04-29 17:12:20",
        |			"is_td": 1
        |		}, {
        |			"id": "2196",
        |			"type": "link",
        |			"title": "盘前早知道",
        |			"relation_id": "https:\/\/niu.sinalicaishi.com.cn\/fe_activity\/index.html#\/themelive0221?id=583",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200712\/2020071223461875be47e20ca759529a97f0179e887e9aef75.jpeg",
        |			"url": "https:\/\/niu.sinalicaishi.com.cn\/fe_activity\/index.html#\/themelive0221?id=583",
        |			"group_id": "1270",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"c_time": "2020-07-07 14:21:00",
        |			"route": {
        |				"type": "link",
        |				"relation_id": "https:\/\/niu.sinalicaishi.com.cn\/fe_activity\/index.html#\/themelive0221?id=583",
        |				"url": "https:\/\/niu.sinalicaishi.com.cn\/fe_activity\/index.html#\/themelive0221?id=583",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}, {
        |			"id": "2020",
        |			"type": "link",
        |			"title": "一键发现目标个股",
        |			"relation_id": "https:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/pickStock.html#\/index",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200706\/20200706065205142349f36fae834dd1cd22d7386ace744614.png",
        |			"url": "https:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/pickStock.html#\/index",
        |			"group_id": "1180",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"c_time": "2020-05-07 18:12:52",
        |			"route": {
        |				"type": "link",
        |				"relation_id": "https:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/pickStock.html#\/index",
        |				"url": "https:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/pickStock.html#\/index",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}],
        |		"entrance": [{
        |			"id": "107822",
        |			"area_code": "173",
        |			"type": "find_planner",
        |			"title": "找老师",
        |			"summary": "",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200709\/20200709013249400019997dbb05a87dae5e87b3488c0b6440.png",
        |			"relation_id": "",
        |			"url": "",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"route": {
        |				"type": "find_planner",
        |				"relation_id": "",
        |				"url": "",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}, {
        |			"id": "107824",
        |			"area_code": "173",
        |			"type": "live_tab",
        |			"title": "看直播",
        |			"summary": "",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200709\/20200709013307150dad20ff3c5d8f1ed0428dfa96a805e015.png",
        |			"relation_id": "1",
        |			"url": "",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"route": {
        |				"type": "live_tab",
        |				"relation_id": "1",
        |				"url": "",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}, {
        |			"id": "107826",
        |			"area_code": "173",
        |			"type": "tab_home_video",
        |			"title": "刷视频",
        |			"summary": "",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200709\/2020070901333357fb6e3b74cf35a93aaf9c7121088904d457.png",
        |			"relation_id": "",
        |			"url": "",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"route": {
        |				"type": "tab_home_video",
        |				"relation_id": "",
        |				"url": "",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}, {
        |			"id": "107816",
        |			"area_code": "173",
        |			"type": "link",
        |			"title": "北向资金",
        |			"summary": "",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200709\/20200709013435246d60a78753a12304419f5c9bd534db1724.png",
        |			"relation_id": "",
        |			"url": "https:\/\/niu.sinalicaishi.com.cn\/fe_activity\/index.html#\/themePage?id=9",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"route": {
        |				"type": "link",
        |				"relation_id": "",
        |				"url": "https:\/\/niu.sinalicaishi.com.cn\/fe_activity\/index.html#\/themePage?id=9",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}, {
        |			"id": "107828",
        |			"area_code": "173",
        |			"type": "tab_home_recommend",
        |			"title": "读资讯",
        |			"summary": "",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200709\/202007090134558464f8ed3cbd5569ba0545ea0dba8a999284.png",
        |			"relation_id": "",
        |			"url": "",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"route": {
        |				"type": "tab_home_recommend",
        |				"relation_id": "",
        |				"url": "",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}, {
        |			"id": "107830",
        |			"area_code": "173",
        |			"type": "link",
        |			"title": "龙虎榜",
        |			"summary": "",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200709\/202007090135475e76eaad607dba5f2bdc6d32b15900b465.png",
        |			"relation_id": "",
        |			"url": "https:\/\/niu.sinalicaishi.com.cn\/lcs\/wap\/dragonAndTiger.html#\/dynamicStar?",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"route": {
        |				"type": "link",
        |				"relation_id": "",
        |				"url": "https:\/\/niu.sinalicaishi.com.cn\/lcs\/wap\/dragonAndTiger.html#\/dynamicStar?",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}, {
        |			"id": "107838",
        |			"area_code": "173",
        |			"type": "link",
        |			"title": "诊股",
        |			"summary": "",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200708\/20200708104530964fabf0552fc9653742f44886211b1a4196.png",
        |			"relation_id": "",
        |			"url": "https:\/\/zhiliao.techgp.cn\/FE_vue_wap\/investingStock.html#\/homepage",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"route": {
        |				"type": "link",
        |				"relation_id": "",
        |				"url": "https:\/\/zhiliao.techgp.cn\/FE_vue_wap\/investingStock.html#\/homepage",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}, {
        |			"id": "107842",
        |			"area_code": "173",
        |			"type": "link",
        |			"title": "理财榜",
        |			"summary": "",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200708\/2020070810460353bfd41f82a7eb20a0ac0c173fb9b04a3c53.png",
        |			"relation_id": "",
        |			"url": "https:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/promotion.html#\/promotion",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"route": {
        |				"type": "link",
        |				"relation_id": "",
        |				"url": "https:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/promotion.html#\/promotion",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}, {
        |			"id": "101186",
        |			"area_code": "191",
        |			"type": "link",
        |			"title": "超级福利",
        |			"summary": "0515替换掉的链接\r\nhttp:\/\/td.licaishisina.com\/business_fe_wap\/?scene=0&clicktime=1587534338#\/act0422?type=0\r\n\r\n0518\r\nhttps:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0515?type=0&bid=10198\r\n\r\n0528\r\nhttps:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0521?type=0&bid=10241\r\n\r\n0604\r\nhttps:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0521?type=0&bid=10231",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200427\/20200427035235385aa72cc0f2c6b5ade8868b97b42bf6d738.png",
        |			"relation_id": "",
        |			"url": "https:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0603?type=0&bid=10231",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"route": {
        |				"type": "link",
        |				"relation_id": "",
        |				"url": "https:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0603?type=0&bid=10231",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}, {
        |			"id": "102884",
        |			"area_code": "191",
        |			"type": "link",
        |			"title": "独门战法",
        |			"summary": "5月18日\r\nhttps:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0515?type=1&bid=10198\r\n\r\n5月28日\r\nhttps:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0515?type=1&bid=10231\r\n\r\n0604\r\nhttps:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0521?type=1&bid=10231",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200515\/20200515013900625aa72cc0f2c6b5ade8868b97b42bf6d762.png",
        |			"relation_id": "",
        |			"url": "https:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0603?type=1&bid=10231",
        |			"isTransparentNavigationBar": "0",
        |			"button_color": "1",
        |			"route": {
        |				"type": "link",
        |				"relation_id": "",
        |				"url": "https:\/\/td.licaishisina.com\/business_fe_wap\/#\/act0603?type=1&bid=10231",
        |				"isTransparentNavigationBar": "0",
        |				"button_color": "1"
        |			}
        |		}],
        |		"market_headlines": {
        |			"data": [{
        |				"type": "view",
        |				"foot": [{
        |					"name": "今日必读",
        |					"is_red": 1
        |				}],
        |				"id": "845684",
        |				"p_uid": "",
        |				"title": "中报季正式开启 高增长公司名单在此",
        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200713\/2020071300445443540f64d66ac643fbfe98657647ba47a743.png",
        |				"p_time": "2020-07-13 08:44:57",
        |				"media2_type": "2",
        |				"media2_url": "",
        |				"cover_images": "undefined",
        |				"video_id": "",
        |				"route": {
        |					"type": "viewdetail",
        |					"relation_id": "845684",
        |					"url": ""
        |				},
        |				"sequence": -1,
        |				"sequence_time": "0000-00-00 00:00:00",
        |				"show_time": "2020-07-13 08:44:57",
        |				"is_read": 0,
        |				"view_media_type": 1
        |			}, {
        |				"type": "new",
        |				"foot": [{
        |					"name": "全网精选",
        |					"is_red": 0
        |				}],
        |				"id": "2495438",
        |				"summary": "【这只“牛基”回报超30倍！还有一批“长跑健将”超10倍、20倍】“以时间换空间，只要耐心持有，终究可以摘到时间的玫瑰。”近期A股强势上行，最牛基金累计回报率已经超30倍。数据显示，截至7月10日，华夏大盘精选成立以来的复权单位净值增长率达3006.93%，两只基金成立以来累计回报率超20倍，还有31只基金业绩超10倍。（中国证券报）",
        |				"title": "这只“牛基”回报超30倍！还有一批“长跑健将”超10倍、20倍",
        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200713\/2020071301200215fbdb0ef92e3ba1f3f2485a7afd8e695415.jpeg",
        |				"p_time": "2020-07-13 05:02:00",
        |				"author": "中国证券报",
        |				"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2495438",
        |				"route": {
        |					"type": "newsdetail",
        |					"relation_id": "2495438",
        |					"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2495438"
        |				},
        |				"sequence": "0",
        |				"sequence_time": "2020-07-13 09:20:05",
        |				"show_time": "2020-07-13 05:02:00",
        |				"is_read": 0
        |			}, {
        |				"type": "new",
        |				"foot": [{
        |					"name": "全网精选",
        |					"is_red": 0
        |				}],
        |				"id": "2495387",
        |				"summary": "【十大券商策略：A股正在经历“长牛” 拥抱权益时代！调整提供加仓良机】兴业证券认为，A股正在经历“长牛”，拥抱权益时代。市场演绎的本质仍是“蓝筹搭台，成长唱戏”，两边都有机会，不是单边的独舞。展望未来，保持观点延续性、持续性和一致性，沿着4月份以来“蓝筹搭台，成长唱戏”的观点，继续看好市场，拥抱权益时代。中信建投认为监管扰动不会导致风格切换戛然而止。当前金融板块出现明显下跌的时候，这反而是加仓的良机。（券商中国）",
        |				"title": "十大券商策略：A股正在经历“长牛” 拥抱权益时代！调整提供加仓良机",
        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200713\/202007130118479016096dd8f284a93270ca7e37bab89d29.jpeg",
        |				"p_time": "2020-07-13 00:16:00",
        |				"author": "券商中国",
        |				"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2495387",
        |				"route": {
        |					"type": "newsdetail",
        |					"relation_id": "2495387",
        |					"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2495387"
        |				},
        |				"sequence": "0",
        |				"sequence_time": "2020-07-13 09:18:51",
        |				"show_time": "2020-07-13 00:16:00",
        |				"is_read": 0
        |			}, {
        |				"type": "new",
        |				"foot": [{
        |					"name": "全网精选",
        |					"is_red": 0
        |				}],
        |				"id": "2494402",
        |				"summary": "【细思极恐！你的聊天内容可能被窃听？一次都没打开的App 却已向外传输数据】现在，我们手机中都安装了各种应用，也就是各种App，的确方便了我们的工作、生活，但是也获取了我们大量的个人信息。一些App的推送能“精准”到你在想什么，它就给你推送什么。App的这种“正合我意”是怎么实现的？个人信息安全有没有风险呢？（央视财经）",
        |				"title": "细思极恐！你的聊天内容可能被窃听？一次都没打开的App 却已向外传输数据",
        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200712\/202007120803145924f4e9998db22104270e9e9bb9a7798559.jpeg",
        |				"p_time": "2020-07-12 15:48:00",
        |				"author": "央视财经",
        |				"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2494402",
        |				"route": {
        |					"type": "newsdetail",
        |					"relation_id": "2494402",
        |					"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2494402"
        |				},
        |				"sequence": "0",
        |				"sequence_time": "2020-07-12 16:03:19",
        |				"show_time": "2020-07-12 15:48:00",
        |				"is_read": 0
        |			}, {
        |				"type": "new",
        |				"foot": [{
        |					"name": "全网精选",
        |					"is_red": 0
        |				}],
        |				"id": "2494378",
        |				"summary": "",
        |				"title": "下周影响市场重要资讯前瞻",
        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200712\/2020071208023449c5fca7eae66bd5cc445f293148fe917f49.jpeg",
        |				"p_time": "2020-07-12 15:14:22",
        |				"author": "新浪财经",
        |				"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2494378",
        |				"route": {
        |					"type": "newsdetail",
        |					"relation_id": "2494378",
        |					"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2494378"
        |				},
        |				"sequence": "0",
        |				"sequence_time": "2020-07-12 16:02:42",
        |				"show_time": "2020-07-12 15:14:22",
        |				"is_read": 0
        |			}, {
        |				"type": "new",
        |				"foot": [{
        |					"name": "全网精选",
        |					"is_red": 0
        |				}],
        |				"id": "2493686",
        |				"summary": "",
        |				"title": "台积电6月营收环比大增 或预示苹果A14处理器已大规模出货",
        |				"image": "http:\/\/n.sinaimg.cn\/spider2020711\/33\/w500h333\/20200711\/e6a4-iwasyek2564517.jpg",
        |				"p_time": "2020-07-11 13:59:23",
        |				"author": "腾讯自选股综合",
        |				"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2493686",
        |				"route": {
        |					"type": "newsdetail",
        |					"relation_id": "2493686",
        |					"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2493686"
        |				},
        |				"sequence": "0",
        |				"sequence_time": "2020-07-11 15:59:14",
        |				"show_time": "2020-07-11 13:59:23",
        |				"is_read": 0
        |			}, {
        |				"type": "new",
        |				"foot": [{
        |					"name": "全网精选",
        |					"is_red": 0
        |				}],
        |				"id": "2493653",
        |				"summary": "",
        |				"title": "A股汽车板块迎来新“一哥”，15块的蔚来你抄底了吗？",
        |				"image": "http:\/\/n.sinaimg.cn\/spider2020711\/235\/w640h395\/20200711\/4817-iwasyek2364388.jpg",
        |				"p_time": "2020-07-11 12:29:45",
        |				"author": "雪球综合",
        |				"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2493653",
        |				"route": {
        |					"type": "newsdetail",
        |					"relation_id": "2493653",
        |					"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2493653"
        |				},
        |				"sequence": "0",
        |				"sequence_time": "2020-07-11 15:58:48",
        |				"show_time": "2020-07-11 12:29:45",
        |				"is_read": 0
        |			}, {
        |				"type": "new",
        |				"foot": [{
        |					"name": "全网精选",
        |					"is_red": 0
        |				}],
        |				"id": "2493561",
        |				"summary": "",
        |				"title": "中国外贸企业出口“转”起来 6月外贸进出口预计由负转正",
        |				"image": "http:\/\/n.sinaimg.cn\/sinakd20200711s\/304\/w551h553\/20200711\/9972-iwasyek2329873.jpg",
        |				"p_time": "2020-07-11 12:32:46",
        |				"author": "华夏时报",
        |				"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2493561",
        |				"route": {
        |					"type": "newsdetail",
        |					"relation_id": "2493561",
        |					"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2493561"
        |				},
        |				"sequence": "0",
        |				"sequence_time": "2020-07-11 12:57:27",
        |				"show_time": "2020-07-11 12:32:46",
        |				"is_read": 0
        |			}, {
        |				"type": "new",
        |				"foot": [{
        |					"name": "全网精选",
        |					"is_red": 0
        |				}],
        |				"id": "2493515",
        |				"summary": "",
        |				"title": "2019券商经营业绩排名来了:全行业净利同比翻番 中信4指标再领跑",
        |				"image": "http:\/\/n.sinaimg.cn\/front20200711ac\/89\/w623h266\/20200711\/325a-iwasyek2139853.jpg",
        |				"p_time": "2020-07-11 11:37:06",
        |				"author": "中国证券报",
        |				"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2493515",
        |				"route": {
        |					"type": "newsdetail",
        |					"relation_id": "2493515",
        |					"url": "http:\/\/niu.sinalicaishi.com.cn\/FE_vue_wap\/newInfo.html#\/newInfo?n_id=2493515"
        |				},
        |				"sequence": "0",
        |				"sequence_time": "2020-07-11 12:03:35",
        |				"show_time": "2020-07-11 11:37:06",
        |				"is_read": 0
        |			}],
        |			"page": 1,
        |			"num": 8,
        |			"total": 0,
        |			"pages": 0,
        |			"title": "市场头条"
        |		},
        |		"recommend": {
        |			"data": [{
        |				"s_uid": "1165644172",
        |				"p_uid": "1165644172",
        |				"name": "张彦芸",
        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200510\/20200510051506335aa72cc0f2c6b5ade8868b97b42bf6d733.png",
        |				"summary": "擅长运用量化工具寻找强势股",
        |				"department": "证券投资咨询业务(投资顾问)",
        |				"position_id": "2",
        |				"sequence": "1000",
        |				"is_live": "0",
        |				"dynamic_sum": 3
        |			}, {
        |				"s_uid": "2090168053",
        |				"p_uid": "2090168053",
        |				"name": "股痴小陈老师",
        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200506\/20200506122551805aa72cc0f2c6b5ade8868b97b42bf6d780.png",
        |				"summary": "震荡行情中发掘逆势牛股",
        |				"department": "证券投资咨询业务(投资顾问)",
        |				"position_id": "2",
        |				"sequence": "1001",
        |				"is_live": "0",
        |				"dynamic_sum": "0"
        |			}, {
        |				"s_uid": "331581924395543",
        |				"p_uid": "331581924395543",
        |				"name": "超短一哥股天乐",
        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200602\/20200602021038565aa72cc0f2c6b5ade8868b97b42bf6d756.jpeg",
        |				"summary": "尾盘决胜超级短线机会",
        |				"department": "理财达人",
        |				"position_id": "27",
        |				"sequence": "1002",
        |				"is_live": "0",
        |				"dynamic_sum": 3
        |			}],
        |			"last_id": "331581924395543",
        |			"type": "recommend",
        |			"title": "大咖推荐"
        |		},
        |		"interesting_talk": {
        |			"title": "股市趣谈",
        |			"data": [{
        |				"planner_id": "6685313102",
        |				"planner_title": "A股直男",
        |				"video_id": "06f7c2376d804cb9bc0861babfcf9ccd",
        |				"video_title": "顶级私募年中策略来袭，看私募机构如何选股，这些股票根本不看！",
        |				"c_time": "2020-07-04 19:51:37",
        |				"video_duration": "645.03",
        |				"image": "http:\/\/s3.licaishi.sina.com.cn\/191108\/1422325693.png",
        |				"sequence": "1001"
        |			}, {
        |				"planner_id": "6677930820",
        |				"planner_title": "A股你莫愁",
        |				"video_id": "e4cbfb72fccd4bb68ff047fb4d03405f",
        |				"video_title": "万亿成交量，股民接下来要知道的事，在这",
        |				"c_time": "2020-07-03 19:45:11",
        |				"video_duration": "249.64",
        |				"image": "http:\/\/s3.licaishi.sina.com.cn\/191108\/1422279268.png",
        |				"sequence": "1000"
        |			}, {
        |				"planner_id": "6408714017",
        |				"planner_title": "股诗百首",
        |				"video_id": "f3bdb5eaebfd480daf258f6602965d01",
        |				"video_title": "分化加剧，科技股的机会在哪里？",
        |				"c_time": "2020-05-29 14:57:01",
        |				"video_duration": "1372.60",
        |				"image": "http:\/\/s3.licaishi.sina.com.cn\/191129\/1634302487.png",
        |				"sequence": "0"
        |			}]
        |		},
        |		"comment_reply": {
        |			"type": "reply",
        |			"id": "",
        |			"msg_type": "",
        |			"title": "收到的回复",
        |			"unread_num": 0,
        |			"sister_unread_num": 0,
        |			"have_new_message": 0,
        |			"switch_status": 1,
        |			"content": "实时接收回复你的消息",
        |			"time": "0000-00-00 00:00:00"
        |		},
        |		"alien_activity": null,
        |		"live_video": {
        |			"title": "视频直播",
        |			"data": [{
        |				"id": "58132",
        |				"sequence": "0",
        |				"title": "定调慢牛而不是疯牛，如何调整交易策略",
        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200609\/20200609031852545aa72cc0f2c6b5ade8868b97b42bf6d754.jpeg",
        |				"name": "罗正享",
        |				"p_uid": "5564048505",
        |				"circle_id": "53726",
        |				"video_id": "",
        |				"start_time": "2020-07-13 09:38:02",
        |				"img_url": "",
        |				"end_time": "2020-07-13 10:38:02",
        |				"tag": "1",
        |				"live_img": "https:\/\/fs.licaishisina.com\/upload\/20200702\/2020070203451835aa72cc0f2c6b5ade8868b97b42bf6d73.png",
        |				"comments": ["买的时候10、14", "老师，现在好了", "有声音了", " $畅联股份(sh603648)$ ", "没有声音的", " $畅联股份(sh603648)$ ", "好了????"],
        |				"router": {
        |					"type": "video_live_room",
        |					"relation_id": "53726",
        |					"video_id": ""
        |				}
        |			}, {
        |				"id": "58131",
        |				"sequence": "0",
        |				"title": "资金决定量价，量价决定涨跌！",
        |				"image": "https:\/\/fs.licaishisina.com\/upload\/20200417\/20200417034604815aa72cc0f2c6b5ade8868b97b42bf6d781.png",
        |				"name": "陈惠",
        |				"p_uid": "1160176332",
        |				"circle_id": "47972",
        |				"video_id": "",
        |				"start_time": "2020-07-13 09:23:48",
        |				"img_url": "",
        |				"end_time": "2020-07-13 10:23:48",
        |				"tag": "1",
        |				"live_img": "https:\/\/fs.licaishisina.com\/upload\/20200702\/20200702032938325aa72cc0f2c6b5ade8868b97b42bf6d732.jpg",
        |				"comments": ["等风等雨分享了直播间", "等风等雨分享了直播间", "华幕初上关注了老师,下次开播会收到通知", "华幕初上关注了老师,下次开播会收到通知", "西关大少关注了老师,下次开播会收到通知", "西关大少关注了老师,下次开播会收到通知", "老师早上好????", "您对未来 $上证指数(sh000001)$ 走势怎么看？", "五连阳榨菜714关注了老师,下次开播会收到通知", "五连阳榨菜714关注了老师,下次开播会收到通知"],
        |				"router": {
        |					"type": "video_live_room",
        |					"relation_id": "47972",
        |					"video_id": ""
        |				}
        |			}],
        |			"expire": 1594604657
        |		},
        |		"suspension": {
        |			"title": "悬浮入口",
        |			"img": "https:\/\/fs.licaishisina.com\/upload\/20200708\/2020070812213751ab6c759f0aaf727f9f9d5093185c498b51.gif",
        |			"img2": "https:\/\/fs.licaishisina.com\/upload\/20200708\/2020070806513400a71f8843d00c21aa2f12dc56c06310a0.png",
        |			"route": {
        |				"type": "goto_wxminipro",
        |				"type_text": "",
        |				"relation_id": "gh_0ab266431788",
        |				"url": "",
        |				"isTransparentNavigationBar": "0",
        |				"path": "pages\/goContactAct\/main?act=act061701&bid=10231&type=2&channel=huaweipay&os=android&deviceid=3c1614a3fc27a542&page=suspension"
        |			}
        |		},
        |		"most_watch": {
        |			"title": "视频解盘",
        |			"data": [{
        |				"title": "7月13日：周末重要数据与重磅讲话",
        |				"video_id": "8568803eab7442bd8777eee318e23a08",
        |				"video_image": "https:\/\/fs.licaishisina.com\/upload\/20200713\/202007130103136820df0cd643c8a58f8092102fe62c9d2768.png",
        |				"video_duration": "132.50",
        |				"unique_value": "dynamic_280031",
        |				"c_time": "2020-07-13 09:22:03",
        |				"p_uid": "1775470803",
        |				"view_num": 2
        |			}, {
        |				"title": "7月9号收盘点评：7天9万亿！资金强势上攻！创业板大涨4%！",
        |				"video_id": "5508f7b5b0394443bc6c2d38e3fff313",
        |				"video_image": "https:\/\/fs.licaishisina.com\/upload\/20200709\/2020070908200426dc19d578c5dfddf2236481e91b5232e2.png",
        |				"video_duration": "203.71",
        |				"unique_value": "dynamic_279590",
        |				"c_time": "2020-07-10 10:33:07",
        |				"p_uid": "1911815605",
        |				"view_num": 775
        |			}, {
        |				"title": "如何躲避未来的大跌——遵守混沌理念",
        |				"video_id": "2960555fd75d4fe69b6b81e3f973f7b8",
        |				"video_image": "https:\/\/fs.licaishisina.com\/upload\/20200709\/2020070907523772ef95cbcabc2713556843330c12d8457472.png",
        |				"video_duration": "277.85",
        |				"unique_value": "dynamic_279584",
        |				"c_time": "2020-07-10 10:32:50",
        |				"p_uid": "1603147504",
        |				"view_num": 465
        |			}, {
        |				"title": "午评！创业板再度雄起，题材唱戏行情，注意去弱留强，坚守强势股",
        |				"video_id": "996fcccfb89b40b1ae92d712648ca9a1",
        |				"video_image": "https:\/\/fs.licaishisina.com\/upload\/20200709\/2020070903220335005ace58c43ad171efa3bbe514f3e85c35.png",
        |				"video_duration": "251.08",
        |				"unique_value": "dynamic_279503",
        |				"c_time": "2020-07-10 10:32:21",
        |				"p_uid": "1995200175",
        |				"view_num": 226
        |			}, {
        |				"title": "难道还有第四波?省广集团强者恒强不停涨！",
        |				"video_id": "76f42f3d1a334a10a284bf65f19e803a",
        |				"video_image": "https:\/\/fs.licaishisina.com\/upload\/20200708\/20200708133541386635d8915cbb05515c5c9f81621bd84838.png",
        |				"video_duration": "361.43",
        |				"unique_value": "dynamic_279340",
        |				"c_time": "2020-07-09 10:06:50",
        |				"p_uid": "3933305924",
        |				"view_num": 533
        |			}]
        |		},
        |		"special": {
        |			"title": "特色专栏",
        |			"data": [{
        |				"p_uid": "787376752043694",
        |				"name": "鲁班行研",
        |				"image": "http:\/\/s3.licaishi.sina.com.cn\/200211\/1136117191.png",
        |				"summary": "跟着鲁班行研看市场，理解行业规律、洞察板块先机。",
        |				"company": "",
        |				"attention": "10148",
        |				"article": {
        |					"type": "view",
        |					"id": "845646",
        |					"title": "业绩大增3倍！5G射频天线龙头将扬帆？",
        |					"image": "https:\/\/fs.licaishisina.com\/upload\/20200710\/20200710103118594fb88dbaf0556ac4f634bd484ffd5eb5.jpg",
        |					"view_media_type": 1,
        |					"p_time": "2020-07-10 18:31:23"
        |				}
        |			}]
        |		},
        |		"famous_planner": {
        |			"title": "金牌投顾",
        |			"data": [{
        |				"p_uid": "6304215484",
        |				"p_name": "郑灿",
        |				"p_image": "https:\/\/fs.licaishisina.com\/upload\/20200330\/2020033009264585aa72cc0f2c6b5ade8868b97b42bf6d78.png",
        |				"bimage": "",
        |				"image_photo": "http:\/\/s3.licaishi.sina.com.cn\/200312\/1652187993.png",
        |				"short_summary": "以敏锐盘感预测主力下一步动向",
        |				"tags": ["实战之星", "主力猎人", "人气专家"],
        |				"entrance": [{
        |					"title": "女神早盘又冲击了涨停， 好东西，要细品！！！",
        |					"image": "",
        |					"type": "dynamicdetail",
        |					"relation_id": "280069",
        |					"url": ""
        |				}, {
        |					"title": "聊天问股",
        |					"image": "http:\/\/s3.licaishi.sina.com.cn\/191108\/1931139493.png",
        |					"type": "planner_tab_chat",
        |					"relation_id": "6304215484",
        |					"url": ""
        |				}, {
        |					"title": "看盘日志",
        |					"image": "http:\/\/s3.licaishi.sina.com.cn\/191108\/1931213045.png",
        |					"type": "planner_tab_video",
        |					"relation_id": "6304215484",
        |					"url": ""
        |				}],
        |				"new_comment": "女神早盘又冲击了涨停， 好东西，要细品！！！",
        |				"comment_type": "动态"
        |			}, {
        |				"p_uid": "6685313102",
        |				"p_name": "股北洪帮主",
        |				"p_image": "http:\/\/s3.licaishi.sina.com.cn\/200310\/0942226156.jpeg",
        |				"bimage": "http:\/\/s3.licaishi.sina.com.cn\/200310\/0942226156.jpeg",
        |				"image_photo": "http:\/\/s3.licaishi.sina.com.cn\/200312\/1829191913.png",
        |				"short_summary": "小散逆袭带头人，让股市没有秘密",
        |				"tags": ["站在风口", "价值投机"],
        |				"entrance": [{
        |					"title": "外资尾盘抢筹，茅台再创新高，7月易涨难跌？",
        |					"image": "https:\/\/fs.licaishisina.com\/upload\/20200629\/20200629105603485917df77f9315bed32f7264ff72c61a548.jpg",
        |					"type": "viewdetail",
        |					"relation_id": "844832",
        |					"url": ""
        |				}],
        |				"new_comment": "外资尾盘抢筹，茅台再创新高，7月易涨难跌？",
        |				"comment_type": "观点"
        |			}, {
        |				"p_uid": "2941018262",
        |				"p_name": "孙立鹏",
        |				"p_image": "http:\/\/s3.licaishi.sina.com.cn\/200208\/1040156589.jpeg",
        |				"bimage": "http:\/\/s3.licaishi.sina.com.cn\/200208\/1040156589.jpeg",
        |				"image_photo": "http:\/\/s3.licaishi.sina.com.cn\/200312\/1659462930.png",
        |				"short_summary": "教你成为专业操盘手",
        |				"tags": ["人气专家", "操盘手导师"],
        |				"entrance": [{
        |					"title": "券商股异动，华鑫股份拉升封板，山西证券、哈投股份、红塔证券等跟涨。",
        |					"image": "",
        |					"type": "dynamicdetail",
        |					"relation_id": "280068",
        |					"url": ""
        |				}],
        |				"new_comment": "券商股异动，华鑫股份拉升封板，山西证券、哈投股份、红塔证券等跟涨。",
        |				"comment_type": "动态"
        |			}, {
        |				"p_uid": "1919069441",
        |				"p_name": "沈波",
        |				"p_image": "http:\/\/s3.licaishi.sina.com.cn\/180\/170727\/1344114108.png",
        |				"bimage": "http:\/\/s3.licaishi.sina.com.cn\/180\/170727\/1344114108.png",
        |				"image_photo": "http:\/\/s3.licaishi.sina.com.cn\/200312\/1636006177.png",
        |				"short_summary": "30年专业视角带你吃透股市",
        |				"tags": ["波段大师", "精准趋势研判"],
        |				"entrance": [{
        |					"title": "主页",
        |					"image": "http:\/\/s3.licaishi.sina.com.cn\/200227\/1906051598.png",
        |					"type": "planner",
        |					"relation_id": 1919069441,
        |					"url": ""
        |				}],
        |				"new_comment": "A股30年牛市里的那些事儿（四）",
        |				"comment_type": "视频直播"
        |			}, {
        |				"p_uid": "851581315317913",
        |				"p_name": "谭彦峰",
        |				"p_image": "http:\/\/s3.licaishi.sina.com.cn\/200229\/1738524997.jpeg",
        |				"bimage": "http:\/\/s3.licaishi.sina.com.cn\/200229\/1738524997.jpeg",
        |				"image_photo": "http:\/\/s3.licaishi.sina.com.cn\/200312\/1726075783.png",
        |				"short_summary": "专注热点，紧随趋势，择时而动",
        |				"tags": ["逻辑推演", "知名投顾"],
        |				"entrance": [{
        |					"title": "这几则周末要闻很重要！",
        |					"image": "https:\/\/fs.licaishisina.com\/upload\/20200419\/202004191126028778805a221a988e79ef3f42d7c5bfd41887.png",
        |					"type": "viewdetail",
        |					"relation_id": "837366",
        |					"url": ""
        |				}],
        |				"new_comment": "这几则周末要闻很重要！",
        |				"comment_type": "观点"
        |			}]
        |		},
        |		"daily_gold_stock": {
        |			"title": "每日金股",
        |			"data": [{
        |				"id": "930",
        |				"title": "医疗器械",
        |				"desc": "各地医疗机构在防控疫情常态化过程中增加战略储备，利好医疗器械板块。",
        |				"image": "http:\/\/s3.licaishi.sina.com.cn\/200325\/2029529887.jpeg",
        |				"stocks": [{
        |					"name": "蓝帆医疗",
        |					"symbol": "sz002382",
        |					"rate": "+19.81%",
        |					"tag": "近3日涨幅",
        |					"is_self_optional": 0
        |				}, {
        |					"name": "佰仁医疗",
        |					"symbol": "sh688198",
        |					"rate": "+15.26%",
        |					"tag": "近3日涨幅",
        |					"is_self_optional": 0
        |				}, {
        |					"name": "三鑫医疗",
        |					"symbol": "sz300453",
        |					"rate": "-9.87%",
        |					"tag": "近3日涨幅",
        |					"is_self_optional": 0
        |				}]
        |			}, {
        |				"id": "877",
        |				"title": "免税店板块",
        |				"desc": "海南离岛免税细则超预期，行业红利持续释放。",
        |				"image": "http:\/\/s3.licaishi.sina.com.cn\/191211\/0046321185.png",
        |				"stocks": [{
        |					"name": "凯撒旅业",
        |					"symbol": "sz000796",
        |					"rate": "+15.47%",
        |					"tag": "近3日涨幅",
        |					"is_self_optional": 0
        |				}, {
        |					"name": "格力地产",
        |					"symbol": "sh600185",
        |					"rate": "-1.99%",
        |					"tag": "近3日涨幅",
        |					"is_self_optional": 0
        |				}, {
        |					"name": "中国中免",
        |					"symbol": "sh601888",
        |					"rate": "-3.48%",
        |					"tag": "近3日涨幅",
        |					"is_self_optional": 0
        |				}]
        |			}, {
        |				"id": "708",
        |				"title": "煤炭板块",
        |				"desc": "近期六部门联合下发《关于做好2020年重点领域化解过剩产能工作的通知》。",
        |				"image": "http:\/\/s3.licaishi.sina.com.cn\/191124\/2141251815.png",
        |				"stocks": [{
        |					"name": "郑州煤电",
        |					"symbol": "sh600121",
        |					"rate": "0.00%",
        |					"tag": "近3日涨幅",
        |					"is_self_optional": 0
        |				}, {
        |					"name": "潞安环能",
        |					"symbol": "sh601699",
        |					"rate": "-2.23%",
        |					"tag": "近3日涨幅",
        |					"is_self_optional": 0
        |				}, {
        |					"name": "安源煤业",
        |					"symbol": "sh600397",
        |					"rate": "-4.98%",
        |					"tag": "近5日涨幅",
        |					"is_self_optional": 0
        |				}]
        |			}]
        |		},
        |		"pop_window": null,
        |		"index_type": "index"
        |	},
        |	"is_login": 0,
        |	"run_time": 0.43836808204651,
        |	"sys_time": "2020-07-13 09:45:03",
        |	"trace_id": "2fpp6iv3tcfh3eabv5988fm55h"
        |}
        |""".stripMargin


    val dataJson = jsonParse(str)
    val dataStr = dataJson.getOrElse("data","")
    val code = dataJson.getOrElse("code","")
    val data2Json = jsonParse(dataStr)
    val bannerStr = data2Json.getOrElse("banner","")
    val jsonParser = new JSONParser()
    val bannerArray = jsonParser.parse("").asInstanceOf[JSONArray]
    val strings: Array[String] = bannerArray.toArray().map(_.toString)
    strings.foreach(println(_))
  }
}
